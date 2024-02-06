# Databricks notebook source
from pyspark.sql.functions import lit

policy_schema= "policy_id int, policy_type string, customer_id int, start_date timestamp, end_date timestamp, premium double,  coverage_amount double"

df = spark.read.json("/mnt/landing/PolicyData/", schema=policy_schema)
display(df)

# COMMAND ----------

df_merge_flag = df.withColumn("merge_flag",lit(False))
display(df_merge_flag)

# COMMAND ----------

df_merge_flag.write.option("path","/mnt/bronzelayer/Policy").mode("append").saveAsTable("bronzelayer.Policy")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronzelayer.Policy

# COMMAND ----------

from datetime import datetime

def getFilePathWithDates(filePath):
    # get the current time in mm-dd-yyyy format
    current_time = datetime.now().strftime('%m-%d-%Y')
    new_file_path = filePath+'/'+current_time
    return new_file_path

# COMMAND ----------

dbutils.fs.mv("/mnt/landing/PolicyData",getFilePathWithDates("/mnt/processed/PolicyData"),True)