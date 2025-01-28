# <ins>**Problem Description:**</ins>

Link: https://leetcode.com/problems/second-highest-salary/description/

Write a solution to find the second highest distinct salary from the Employee table. If there is no second highest salary, return null (return None in Pandas).

<ins>**SQL DDL:**</ins>

Create table If Not Exists Employee (id int, salary int);\
Truncate table Employee;

insert into Employee (id, salary) values ('1', '100');\
insert into Employee (id, salary) values ('2', '200');\
insert into Employee (id, salary) values ('3', '300');



<ins>**PySpark DDL:**</ins>


data = [[1, 100], [2, 200], [3, 300]]

df = spark.createDataFrame(data, ["id", "salary"])

<ins>**Python DDL:**</ins>

import pandas as pd

data = [[1, 100], [2, 200], [3, 300]]

employee = pd.DataFrame(data, columns=['id', 'salary']).astype({'id':'int64', 'salary':'int64'})