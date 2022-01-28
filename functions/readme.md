### Window function:
https://www.geeksforgeeks.org/pyspark-window-functions/

To perform window function operation on a group of rows first, we need to **partition** 

- define the group of data rows using window.partition() function
- for row number and rank function we need to additionally order by on partition data using ORDER BY clause. 

Syntax for Window.partition:
```python
Window.partitionBy(“column_name”).orderBy(“column_name”)
```

Example
```python
# importing pyspark
from pyspark.sql.window import Window
import pyspark

# importing sparksessio
from pyspark.sql import SparkSession

# creating a sparksession object
# and providing appName
spark = SparkSession.builder.appName("pyspark_window").getOrCreate()

# sample data for dataframe
sampleData = (("Ram", 28, "Sales", 3000),
			("Meena", 33, "Sales", 4600),
			("Robin", 40, "Sales", 4100),
			("Kunal", 25, "Finance", 3000),
			("Ram", 28, "Sales", 3000),
			("Srishti", 46, "Management", 3300),
			("Jeny", 26, "Finance", 3900),
			("Hitesh", 30, "Marketing", 3000),
			("Kailash", 29, "Marketing", 2000),
			("Sharad", 39, "Sales", 4100)
			)

# column names for dataframe
columns = ["Employee_Name", "Age",
		"Department", "Salary"]

# creating the dataframe df
df = spark.createDataFrame(data=sampleData,
						schema=columns)

# importing Window from pyspark.sql.window

# creating a window
# partition of dataframe
windowPartition = Window.partitionBy("Department").orderBy("Age")

# print schema
df.printSchema()

# show df
df.show()

```

Example 2:
```python
# importing pyspark
from pyspark.sql.window import Window
import pyspark

# importing sparksessio
from pyspark.sql import SparkSession

# creating a sparksession object and providing appName
spark = SparkSession.builder.appName("pyspark_window").getOrCreate()

# sample data for dataframe
sampleData = ((101, "Ram", "Biology", 80),
			(103, "Meena", "Social Science", 78),
			(104, "Robin", "Sanskrit", 58),
			(102, "Kunal", "Phisycs", 89),
			(101, "Ram", "Biology", 80),
			(106, "Srishti", "Maths", 70),
			(108, "Jeny", "Physics", 75),
			(107, "Hitesh", "Maths", 88),
			(109, "Kailash", "Maths", 90),
			(105, "Sharad", "Social Science", 84)
			)

# column names for dataframe
columns = ["Roll_No", "Student_Name", "Subject", "Marks"]

# creating the dataframe df
df2 = spark.createDataFrame(data=sampleData,
							schema=columns)

# importing window from pyspark.sql.window

# creating a window partition of dataframe
windowPartition = Window.partitionBy("Subject").orderBy("Marks")

# print schema
df2.printSchema()

# show df
df2.show()

```

Example 3
```python
# importing pyspark
import pyspark
 
# importing sparksessio
from pyspark.sql import SparkSession
 
# creating a sparksession
# object and providing appName
spark = SparkSession.builder.appName("pyspark_window").getOrCreate()
 
# sample data for dataframe
sampleData = (("Ram", "Sales", 3000),
              ("Meena", "Sales", 4600),
              ("Robin", "Sales", 4100),
              ("Kunal", "Finance", 3000),
              ("Ram", "Sales", 3000),
              ("Srishti", "Management", 3300),
              ("Jeny", "Finance", 3900),
              ("Hitesh", "Marketing", 3000),
              ("Kailash", "Marketing", 2000),
              ("Sharad", "Sales", 4100)
              )
 
# column names for dataframe
columns = ["Employee_Name", "Department", "Salary"]
 
# creating the dataframe df
df3 = spark.createDataFrame(data=sampleData,
                            schema=columns)
 
# print schema
df3.printSchema()
 
# show df
df3.show()
```

Syntax for Window function:
```python
DataFrame.withColumn(“new_col_name”, Window_function().over(Window_partition))
```

Three types of window functions:
#### Analytical Function
- An analytic function is a function that returns a result after operating on data or a finite set of rows partitioned by a SELECT clause or in the ORDER BY clause. - It returns a result in the same number of rows as the number of input rows. E.g. `lead()`, `lag()`, `cume_dist()`.

Example 1: Using `cume_dist()`
`cume_dist()` window function is used to get the cumulative distribution within a window partition. It is similar to CUME_DIST in SQL. Let’s see an example:
```python
# importing cume_dist()
# from pyspark.sql.functions
from pyspark.sql.functions import cume_dist

# applying window function with
# the help of DataFrame.withColumn
df.withColumn("cume_dist",
			cume_dist().over(windowPartition)).show()

```

#### Ranking Function
- The function returns the statistical rank of a given value for each row in a partition or group. 
- The goal of this function is to provide consecutive numbering of the rows in the resultant column, set by the order selected in the Window.partition for each partition specified in the `OVER` clause. E.g. `row_number()`, `rank()`, `dense_rank()`, etc.

Example 1: Using `row_number()`.
`row_number()` function is used to gives a sequential number to each row present in the table. Let’s see the example:
```python
# importing row_number() from pyspark.sql.functions
from pyspark.sql.functions import row_number
 
# applying the row_number() function
df2.withColumn("row_number",
               row_number().over(windowPartition)).show()
```
Example 2: Using `rank()`
The rank function is used to give ranks to rows specified in the window partition. This function leaves gaps in rank if there are ties. Let’s see the example:
```python
# importing rank() from pyspark.sql.functions
from pyspark.sql.functions import rank

# applying the rank() function
df2.withColumn("rank", rank().over(windowPartition)) \
	.show()
#give the rank for each subject by their markes
```

#### Aggregate Function
- A function where the values of multiple rows are grouped to form a single summary value. 
- The definition of the groups of rows on which they operate is done by using the SQL `GROUP BY` clause. E.g. AVERAGE, SUM, MIN, MAX, etc. 

Example
```python
# importing window from pyspark.sql.window
from pyspark.sql.window import Window
 
# importing aggregate functions
# from pyspark.sql.functions
from pyspark.sql.functions import col,avg,sum,min,max,row_number
 
# creating a window partition of dataframe
windowPartitionAgg  = Window.partitionBy("Department")
 
# applying window aggregate function
# to df3 with the help of withColumn
 
# this is average()
df3.withColumn("Avg",
               avg(col("salary")).over(windowPartitionAgg))
    #this is sum()
  .withColumn("Sum",
              sum(col("salary")).over(windowPartitionAgg))
    #this is min()
  .withColumn("Min",
              min(col("salary")).over(windowPartitionAgg))
    #this is max()
  .withColumn("Max",
              max(col("salary")).over(windowPartitionAgg)).show()

```
