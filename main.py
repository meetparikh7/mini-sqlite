import sqlparse
import sys

class Query():
    def __init__(self, query):
        self.query = sqlparse.parse(query.strip())[0]

    def debug(self):
        print(self.query.tokens)

if __name__ == "__main__":
    Query(sys.argv[1]).debug()
