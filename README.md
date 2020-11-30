# Mini SQLite

A mini-sqlite like database made for understanding the query execution engine

Example queries

```sql
-- select everything from table A
SELECT * FROM A
-- select everything from table A and B
SELECT * FROM A, B
-- select two rows (col1, colX) from A and B
SELECT col1, colX FROM A, B
-- select everything from table A and B, join on col1 and colX
SELECT * FROM A, B WHERE col1=colX
-- select distinct rows (colX, colY) from B
SELECT DISTINCT colX, colY FROM B
-- select rows (colX, colZ) from B where colX=98 and colZ=7
SELECT colX, colZ FROM B WHERE colX=98 and colZ=7
-- select rows (colX, colZ, col1) from A and B where (colX=98 and colZ=7) OR col1>2
SELECT colX, colZ, col1 FROM A, B WHERE (colX=98 AND colZ=7) OR col1>2
-- select rows (colX, colZ) from A and B where (colX=98 and colZ=7) OR col1>2
SELECT colX, colZ FROM A, B WHERE (colX=98 AND colZ=7) OR col1 > 2
```
