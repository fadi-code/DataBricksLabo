# Databricks notebook source
dbutils.fs.ls("/mnt/silver/SalesLT/")

# COMMAND ----------

dbutils.fs.ls("/mnt/gold")

# COMMAND ----------

input_path= '/mnt/silver/SalesLT/Address/'

# COMMAND ----------

df = spark.read.format('delta').load(input_path)

# COMMAND ----------

display(df)

# COMMAND ----------


from pyspark.sql import SparkSession

# Import necessary functions
from pyspark.sql.functions import col, regexp_replace

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# List to store table names
table_names = []

# Get table names from the specified directory
for i in dbutils.fs.ls("/mnt/silver/SalesLT/"):
    table_names.append(i.name.split('/')[0])

# Process each table
for name in table_names:
    path = f'/mnt/silver/SalesLT/{name}'
    print(path)
    df = spark.read.format('delta').load(path)  # Use the path variable here
    
    # List of column names
    column_names = df.columns

    for old_col_name in column_names:
        new_col_name = ''.join(["_" + char if char.isupper() and (i != 0 and not old_col_name[i - 1].isupper()) else char for i, char in enumerate(old_col_name)]).lstrip("_")
        
        # Rename the column using withColumnRenamed
        df = df.withColumnRenamed(old_col_name, new_col_name)

    # Output path
    out_path = f'/mnt/gold/SalesLT/{name}/'
    df.write.format('delta').mode('overwrite').save(out_path)




    """ from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

Table_name = []
for i in dbutils.fs.ls("/mnt/silver/SalesLT/"):
    Table_name.append(i.name.split('/')[0])


path= '/mnt/silver/SalesLT/Address/'
df = spark.read.format('delta').load(input_path) 



# Liste des noms de colonnes
column_names = df.columns

for name in Table_name :
 path= '/mnt/silver/SalesLT/Address/' + name
 print(path)
 df = spark.read.format('delta').load(input_path) 

column_names = df.columns

 for old_col_name in column_names : 
    new_col_name = ''.join(["_" + char if char.isupper() and (i != 0 and not old_col_name[i - 1].isupper()) else char for i, char in enumerate(old_col_name)]).lstrip("_")
    
    # Changer le nom de la colonne en utilisant withColumnRenamed et regexp_replace
    df = df.withColumnRenamed(old_col_name, new_col_name)

    out_path ='/mnt/gold/SalesLT/' + name + '/'
    df.write.format('delta').mode('overwrite').save(out_path) """



# COMMAND ----------


