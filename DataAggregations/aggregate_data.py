from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os

from pyspark.sql.functions import expr, avg, asc
from pyspark.sql import functions as f
from pyspark.pandas import DataFrame as dd

python_executable = r"C:\Users\Consultant\AppData\Local\Programs\Python\Python310\python.exe"
os.environ['PYSPARK_PYTHON'] = python_executable

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ReadCSV") \
    .getOrCreate()

# Read the CSV file into a DataFrame
print("First time data")
df = spark.read.option("header", "true").csv("sample_data.csv")
# Show the DataFrame
df.show()

"""Transformation 1: Selecting specific columns (name, age)"""
df_age = df.select("Age")
df_age.show()

"""Transformation 2: Filtering rows based on a condition Age >30"""
age_greater_30 = df_age.filter(df.Age > 30)
age_greater_30.show()

"""Transformation 3: Adding a new column (Salary+10000)"""
adding_10000 = df.withColumn("Salary", expr("Salary + 1000"))
adding_10000.show()

""" Transformation 4: Grouping and aggregating data, based on age calculate average salary"""
average_salary_by_age = df.groupBy("Age").agg(avg("Salary").alias("AverageSalary"))
average_salary_by_age.show()

"""Transformation 5: Sorting by a column (order by age)"""
sort_by_age = df.orderBy(asc("Age"))
sort_by_age.show()

"""Transformation 6: Adding a new column with a conditional expression( if age >30 "Yes" else "No")"""
ages_greater_than_30 = df.withColumn(" > 30",
                                     f.when(f.col("Age") > 30,
                                            "YES").otherwise("NO"))
ages_greater_than_30.show()

"""Transformation 7: Dropping a column (drop salary)"""
drop_salary = ages_greater_than_30.drop(f.col("Salary"))
drop_salary.show()

"""Transformation 8: Renaming columns (name to full name)"""
renamed_column = df.withColumnRenamed("Name", "Full name")
renamed_column.show()

"""Transformation 9: Union of two DataFrames"""
df2 = spark.createDataFrame(data=[("Mike", 40, 80000), ],
                            schema=["Name", "Age", "Salary"])

print("Merged datasets")
merged = df.union(df2)
merged.show()

# Stop the Spark session
spark.stop()
