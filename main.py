import csv
import os
import sqlparse
import sys


def print_table(cols, data):
    for column in cols:
        print(f"{column}\t", end="")
    print("")
    for row in data:
        for column in row:
            print(f"{column}\t", end="")
        print("")


class Tables:
    def __init__(self, dirpath):
        self.tables = {}
        with open(os.path.join(dirpath, "metadata.txt")) as f:
            parsing_table = None
            cols = []
            for line in f:
                og_line = line
                line = line.strip()
                if line.startswith("<"):
                    parsed = line[1:-1].strip().split(" ")
                    if parsed[0] == "begin":
                        parsing_table = parsed[1]
                    elif parsed[0] == "end":
                        self.tables[parsing_table] = Table(parsing_table, cols, dirpath)
                        parsing_table = None
                        cols = []
                    else:
                        raise ValueError("Invalid line", og_line)
                elif parsing_table is not None:
                    cols.append(line)
                else:
                    raise ValueError("Invalid line", og_line)

    def __getitem__(self, key):
        return self.tables[key]

    def debug(self):
        for table in self.tables.values():
            table.debug()
            print("")


class Table:
    def __init__(self, name, cols, dirpath):
        self.name = name
        self.cols = cols
        self.data = []
        with open(os.path.join(dirpath, f"{name}.csv")) as f:
            reader = csv.reader(f)
            for row in reader:
                row = [int(v) for v in row]
                self.data.append(row)

    def debug(self):
        print(f"\nPrinting table {self.name}\n")
        print_table(self.cols, self.data)


class Query:
    def __init__(self, query_str, all_tables):
        self.all_tables = all_tables
        self.query = sqlparse.parse(query_str.strip())[0]
        self.query.tokens = [
            token
            for token in self.query.tokens
            if token.ttype != sqlparse.tokens.Whitespace
        ]
        print(self.query.tokens)
        print([token.ttype for token in self.query.tokens])

        # Hacky parsing of the query
        if self.query.get_type() != "SELECT":
            raise NotImplementedError("Only select query is supported as of now")

        it = iter(self.query.tokens)
        self.tables = []
        self.cols = "*"
        self.distinct = False
        self.where_clause = None
        for token in it:
            if (
                token.ttype == sqlparse.tokens.Keyword.DML
                and token.value.upper() == "SELECT"
            ):
                token = next(it, None)
                if (
                    type(token) is sqlparse.sql.Token
                    and token.value.upper() == "DISTINCT"
                ):
                    self.distinct = True
                    token = next(it, None)
                if type(token) is sqlparse.sql.Identifier:
                    self.cols = [token.value]
                elif type(token) is sqlparse.sql.IdentifierList:
                    self.cols = [token.value for token in token.get_identifiers()]
                elif type(token) is sqlparse.sql.Token and token.value == "*":
                    self.cols = "*"
                else:
                    print(type(token), token)
            elif (
                token.ttype == sqlparse.tokens.Keyword and token.value.upper() == "FROM"
            ):
                token = next(it, None)
                if type(token) is sqlparse.sql.Identifier:
                    self.tables = [token.value]
                elif type(token) is sqlparse.sql.IdentifierList:
                    self.tables = [token.value for token in token.get_identifiers()]
                else:
                    raise ValueError(
                        "No table after from clause in query \n %s" % (query_str)
                    )
            elif type(token) == sqlparse.sql.Where:
                self.where_clause = self.parse_condition_clause(token.tokens[1:])
                print("where_clause", self.where_clause)

        self.check_tables()

    # Creates a lispy expr tree
    def parse_condition_clause(self, condition_clause):
        # Remove whitespace
        condition_clause = [
            token
            for token in condition_clause
            if token.ttype != sqlparse.tokens.Whitespace
        ]

        # Handle parenthesis
        while (
            len(condition_clause) == 1
            and type(condition_clause[0]) == sqlparse.sql.Parenthesis
        ):
            condition_clause = condition_clause[0].tokens[1:-1]
            condition_clause = [
                token
                for token in condition_clause
                if token.ttype != sqlparse.tokens.Whitespace
            ]

        # There are two ways of simple comparison
        simple_condition_tokens = []
        if (
            len(condition_clause) == 1
            and type(condition_clause[0]) == sqlparse.sql.Comparison
        ):
            simple_condition_tokens = condition_clause[0].tokens
            simple_condition_tokens = [
                token
                for token in simple_condition_tokens
                if token.ttype != sqlparse.tokens.Whitespace
            ]
        elif (
            len(condition_clause) == 3
            and condition_clause[1].ttype == sqlparse.tokens.Operator.Comparison
        ):
            simple_condition_tokens = condition_clause
        # Simple comparisions
        if len(simple_condition_tokens) == 3:
            op = simple_condition_tokens[1].value
            # Operand must be colname or int, since only int values in database
            operand1 = (
                simple_condition_tokens[0].value
                if type(simple_condition_tokens[0]) is sqlparse.sql.Identifier
                else int(simple_condition_tokens[0].value)
            )
            operand2 = (
                simple_condition_tokens[2].value
                if type(simple_condition_tokens[2]) is sqlparse.sql.Identifier
                else int(simple_condition_tokens[2].value)
            )
            return (op, operand1, operand2)

        # Compound stataments
        if len(condition_clause) == 3 and condition_clause[1].value.upper() in [
            "AND",
            "OR",
        ]:
            return (
                condition_clause[1].value.upper(),
                self.parse_condition_clause([condition_clause[0]]),
                self.parse_condition_clause([condition_clause[2]]),
            )
        return ()

    def check_tables(self):
        for table in self.tables:
            if table not in self.all_tables.tables.keys():
                raise ValueError("Invalid table %s in query" % (table))

    def debug(self):
        print("Debugging query", self.query)
        print("Selecting")
        print("Cols", self.cols)
        print("From", self.tables)

    def join_tables(self, tables):
        # Assume temporarily only one table
        cur_vtable = []
        cur_vtable_cols = []
        table_name = tables[0]
        table = self.all_tables[table_name]
        cols_to_keep = []
        for col_index, col in enumerate(table.cols):
            if f"{table_name}.{col}" in self.cols:
                cols_to_keep.append(col_index)
                cur_vtable_cols.append(f"{table_name}.{col}")
            elif col in self.cols:
                cols_to_keep.append(col_index)
                cur_vtable_cols.append(col)
            elif self.cols == "*":
                cols_to_keep.append(col_index)
                if col in self.cols:
                    cur_vtable_cols.append(f"{table_name}.{col}")
                else:
                    cur_vtable_cols.append(col)
        for row in table.data:
            filtered_row = [
                cell for index, cell in enumerate(row) if index in cols_to_keep
            ]
            cur_vtable.append(filtered_row)
        if len(self.vtable) == 0:
            self.vtable = cur_vtable
            self.vtable_cols = cur_vtable_cols
        else:
            new_vtable = []
            for row1 in self.vtable:
                for row2 in cur_vtable:
                    new_row = [*row1, *row2]
                    new_vtable.append(new_row)
            self.vtable_cols += cur_vtable_cols
            self.vtable = new_vtable
        if len(tables) > 1:
            self.join_tables(tables[1:])

    # Uses lispy condition, to check a row, (nested) tuples of form (operator, operand1, operand2)
    def check_row(self, row, cols, condition):
        def get_comparision_lambda(op):
            if op == ">":
                return lambda op1, op2: op1 > op2
            elif op == "<":
                return lambda op1, op2: op1 < op2
            elif op == ">=":
                return lambda op1, op2: op1 >= op2
            elif op == "<=":
                return lambda op1, op2: op1 <= op2
            elif op == "!=" or op == "~=":
                return lambda op1, op2: op1 != op2
            else:
                return lambda op1, op2: op1 == op2

        if len(condition) != 3:  # Some invalid condition, dont accept any row
            return False
        if condition[0] == "AND":
            return self.check_row(row, cols, condition[1]) and self.check_row(
                row, cols, condition[2]
            )
        elif condition[0] == "OR":
            return self.check_row(row, cols, condition[1]) or self.check_row(
                row, cols, condition[2]
            )
        elif type(condition[1]) is str:
            op = condition[0]
            field1 = row[cols.index(condition[1])]
            if type(condition[2]) is str:
                field2 = row[cols.index(condition[2])]
            else:
                field2 = condition[2]
            return get_comparision_lambda(op)(field1, field2)
        else:
            return False

    def filter_rows(self):
        if self.where_clause is None:
            return
        new_vtable = []
        for row in self.vtable:
            if self.check_row(row, self.vtable_cols, self.where_clause):
                new_vtable.append(row)
        self.vtable = new_vtable

    def filter_distinct(self):
        if not self.distinct:
            return
        new_vtable = []
        for row in self.vtable:
            if row in new_vtable:
                continue
            new_vtable.append(row)
        self.vtable = new_vtable

    def execute(self):
        self.vtable = []
        self.vtable_cols = []
        self.join_tables(self.tables)
        self.filter_rows()
        self.filter_distinct()
        return self.vtable_cols, self.vtable


if __name__ == "__main__":
    tables = Tables(sys.argv[1])
    tables.debug()
    query = Query(sys.argv[2], tables)
    query.debug()
    result_cols, result_data = query.execute()
    print_table(result_cols, result_data)
