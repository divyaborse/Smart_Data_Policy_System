# Databricks notebook source

branch_schema = "branch_id int, branch_country string, branch_city string"
df = spark.read.parquet("/mnt/landing/BranchData/*.parquet", inferSchema=False, schema=branch_schema)
display(df)


# COMMAND ----------

from pyspark.sql.functions import lit
df_merge_flag = df.withColumn("merge_flag", lit(False))
display(df_merge_flag)
df_merge_flag.write.option("path","/mnt/bronzelayer/Branch").mode("append").saveAsTable("bronzelayer.Branch")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronzelayer.Branch

# COMMAND ----------

from datetime import datetime

def getFilePathWithDates(filePath):
    # get the current time in mm-dd-yyyy format
    current_time = datetime.now().strftime('%m-%d-%Y')
    new_file_path = filePath+'/'+current_time
    return new_file_path

# COMMAND ----------

dbutils.fs.mv("/mnt/landing/BranchData/", getFilePathWithDates('/mnt/processed/BranchData'), True)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/processed/BranchData