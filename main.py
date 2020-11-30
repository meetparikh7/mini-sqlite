import csv
import os
import sqlparse
import sys


class Tables:
    def __init__(self, dirpath):
        self.tables = []
        with open(os.path.join(dirpath, "metadata.txt")) as f:
            parsing_table = None
            columns = []
            for line in f:
                og_line = line
                line = line.strip()
                if line.startswith("<"):
                    parsed = line[1:-1].strip().split(" ")
                    if parsed[0] == "begin":
                        parsing_table = parsed[1]
                    elif parsed[0] == "end":
                        self.tables.append(Table(parsing_table, columns, dirpath))
                        parsing_table = None
                        columns = []
                    else:
                        raise ValueError("Invalid line", og_line)
                elif parsing_table is not None:
                    columns.append(line)
                else:
                    raise ValueError("Invalid line", og_line)

    def debug(self):
        for table in self.tables:
            table.debug()
            print("")


class Table:
    def __init__(self, name, columns, dirpath):
        self.name = name
        self.columns = columns
        self.data = []
        with open(os.path.join(dirpath, f"{name}.csv")) as f:
            reader = csv.reader(f)
            for row in reader:
                self.data.append(row)

    def debug(self):
        print(f"\nPrinting table {self.name}\n")
        for column in self.columns:
            print(f"{column}\t", end="")
        print("")
        for row in self.data:
            for column in row:
                print(f"{column}\t", end="")
            print("")


class Query:
    def __init__(self, query):
        self.query = sqlparse.parse(query.strip())[0]

    def debug(self):
        print(self.query.tokens)


if __name__ == "__main__":
    Tables(sys.argv[1]).debug()
    Query(sys.argv[2]).debug()
