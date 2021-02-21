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
                self.data.append(row)

    def debug(self):
        print(f"\nPrinting table {self.name}\n")
        print_table(self.cols, self.data)


class Query:
    def __init__(self, query_str, all_tables):
        self.all_tables = all_tables
        self.query = sqlparse.parse(query_str.strip())[0]
        self.query.tokens = [token for token in self.query.tokens if token.ttype != sqlparse.tokens.Whitespace]
        print(self.query.tokens)
        print([token.ttype for token in self.query.tokens])

        # Hacky parsing of the query
        if self.query.get_type() != "SELECT":
            raise NotImplementedError('Only select query is supported as of now')

        it = iter(self.query.tokens)
        self.tables = []
        self.cols = "*"
        for token in it:
            if token.ttype == sqlparse.tokens.Keyword.DML and token.value.upper() == "SELECT":
                token = next(it, None)
                if type(token) is sqlparse.sql.Identifier:
                    self.cols = [token.value]
                elif type(token) is sqlparse.sql.IdentifierList:
                    self.cols = [token.value for token in token.get_identifiers()]
                elif type(token) is sqlparse.sql.Token and token.value == "*":
                    self.cols = "*"
                else:
                    print(type(token), token)
            elif token.ttype == sqlparse.tokens.Keyword and token.value.upper() == "FROM":
                token = next(it, None)
                if type(token) is sqlparse.sql.Identifier:
                    self.tables = [token.value]
                elif type(token) is sqlparse.sql.IdentifierList:
                    self.tables = [token.value for token in token.get_identifiers()]
                else:
                    raise ValueError('No table after from clause in query \n %s' % (query_str))

        self.check_tables()

    def check_tables(self):
        for table in self.tables:
            if table not in self.all_tables.tables.keys():
                raise ValueError('Invalid table %s in query' % (table))

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
            filtered_row = [cell for index, cell in enumerate(row) if index in cols_to_keep]
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

    def execute(self):
        self.vtable = []
        self.vtable_cols = []
        self.join_tables(self.tables)
        return self.vtable_cols, self.vtable


if __name__ == "__main__":
    tables = Tables(sys.argv[1])
    tables.debug()
    query = Query(sys.argv[2], tables)
    query.debug()
    result_cols, result_data = query.execute()
    print_table(result_cols, result_data)
