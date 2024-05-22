# Databricks notebook source
# Check if the directory is already mounted
if not any(mount.mountPoint == "/mnt/bronze" for mount in dbutils.fs.mounts()):
    # Directory is not mounted, so mount it
    dbutils.fs.mount(
        source="abfss://bronze@datalakelabo4.dfs.core.windows.net/",
        mount_point="/mnt/bronze",
        extra_configs=configs
    )
else:
    print("Directory /mnt/bronze is already mounted.")

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze/SalesLT")

# COMMAND ----------

# Check if the directory is already mounted
if not any(mount.mountPoint == "/mnt/silver" for mount in dbutils.fs.mounts()):
    # Directory is not mounted, so mount it
    dbutils.fs.mount(
        source="abfss://silver@datalakelabo4.dfs.core.windows.net/",
        mount_point="/mnt/silver",
        extra_configs=configs
    )
else:
    print("Directory /mnt/silver is already mounted.")

# COMMAND ----------

dbutils.fs.ls("/mnt/silver")

# COMMAND ----------

dbutils.fs.ls('/mnt/bronze/SalesLT/')

# COMMAND ----------

Table_name = []
for i in dbutils.fs.ls("/mnt/bronze/SalesLT/"):
    Table_name.append(i.name.split('/')[0])

# COMMAND ----------

Table_name

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

for i in Table_name:
    path = '/mnt/bronze/SalesLT/' + i + '/' + i + '.parquet '
    df = spark.read.format('parquet').load(path)
    columns = df.columns
    for col in columns:
        if "Date" in col or "date" in col:
            df = df.withColumn(col, date_format(from_utc_timestamp(df[col].cast(TimestampType()), "UTC"), "yyyy-MM-dd"))
    output_path = '/mnt/silver/SalesLT/' + i + '/'
    df.write.format("delta").mode("overwrite").save(output_path)


# COMMAND ----------

input_path='dbfs:/mnt/bronze/SalesLT/Address/'

df=spark.read.format('parquet').load(input_path)

display(df) 

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze/SalesLT/Address/")
