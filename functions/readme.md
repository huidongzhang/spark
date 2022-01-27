### Window function:
Three types:
- Analytical Function
- Ranking Function
- Aggregate Function

To perform window function operation on a group of rows first, we need to **partition** 

- define the group of data rows using window.partition() function
- for row number and rank function we need to additionally order by on partition data using ORDER BY clause. 

Syntax for Window.partition:
```python
Window.partitionBy(“column_name”).orderBy(“column_name”)

Syntax for Window function:

DataFrame.withColumn(“new_col_name”, Window_function().over(Window_partition))
```
