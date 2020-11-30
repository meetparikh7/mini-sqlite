import os
import sqlparse
import sys


class Tables:
    def __init__(self, dirpath):
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
                        print("New table", parsing_table)
                    elif parsed[0] == "end":
                        print("End table", parsing_table, "columns", columns)
                        parsing_table = None
                        columns = []
                    else:
                        raise ValueError("Invalid line", og_line)
                elif parsing_table is not None:
                    columns.append(line)
                else:
                    raise ValueError("Invalid line", og_line)


class Query:
    def __init__(self, query):
        self.query = sqlparse.parse(query.strip())[0]

    def debug(self):
        print(self.query.tokens)


if __name__ == "__main__":
    Tables(sys.argv[1])
    Query(sys.argv[2]).debug()
