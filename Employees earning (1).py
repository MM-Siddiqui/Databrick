# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC 
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/employee_earnings_report_2016-6.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "employee_earnings_report"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select * from `employee_earnings_report`

# COMMAND ----------

## total number of males and females of each profession present in each department

# COMMAND ----------

# MAGIC %sql 
# MAGIC select  count(Title) as Male, Department_Name,title
# MAGIC from employee_earnings_report
# MAGIC where Gender='M'
# MAGIC group by Title,Department_Name

# COMMAND ----------

# MAGIC %sql 
# MAGIC select  count(Title) as Female, Department_Name,title
# MAGIC from employee_earnings_report
# MAGIC where Gender='F'
# MAGIC group by Title,Department_Name

# COMMAND ----------

##the top 3 postal code(those postal code which has high count)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(postal) as Number_Postols,postal
# MAGIC from employee_earnings_report
# MAGIC Group by Postal 
# MAGIC order by Number_Postols desc
# MAGIC Limit(3)

# COMMAND ----------

## top 3 postal code(those postal code which has high count) of each profession present in each department

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(postal) as Highest_Postol_counts, title,Department_name
# MAGIC from employee_earnings_report
# MAGIC Group by title,Department_name
# MAGIC order by Highest_Postol_counts desc
# MAGIC limit(3)

# COMMAND ----------

## Resolving queries throgh dataframe

# COMMAND ----------

employee_earnings_report = spark.read.format(file_type) \
                       .option("inferSchema", infer_schema) \
                       .option("header", first_row_is_header) \
                       .option("sep", delimiter) \
                       .load(file_location)

# COMMAND ----------

display(employee_earnings_report)

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

##---total number of males & Females inn profession---

# COMMAND ----------



# COMMAND ----------

employee_earnings_report.groupBy('Title','Department_Name','Gender').agg(F.count('Gender').alias('Males'),F.col('Gender')).where(F.col('Gender')=='M'.show()

# COMMAND ----------

employee_earnings_report.groupBy('Title','Department_Name,'Gender').agg(F.count('Title').alias('Males')).orderBy(F.col('Males').desc()).where(F.col('Gender')=='M'.show()


# COMMAND ----------

##-------Top 3 Postal codes---

# COMMAND ----------

employee_earnings_report.groupBy('postal').agg(F.count('postal').alias('No_of_counts')).orderBy(F.col('No_of_counts').desc()).limit(3).show()

# COMMAND ----------

###----Top 3 postal codes inn each profession & despartment---

# COMMAND ----------

employee_earnings_report.groupBy('Title','Department_Name').agg(F.count('postal').alias('No_of_counts')).orderBy(F.col('No_of_counts').desc()).limit(3).show()

# COMMAND ----------

##// write dataframe to CSV file

# COMMAND ----------

employee_earnings_report.write.option("header",True).csv("/FileStore/tables/employee_earnings_report-2021_.csv")

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "employee_earnings_report_2016-6_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)
