# Databricks notebook source
schema = "agent_id integer, agent_name string, agent_email string,agent_phone string, branch_id integer, create_timestamp timestamp"
df = spark.read.parquet("/mnt/landing/AgentData/*.parquet")
display(df)

# COMMAND ----------

from pyspark.sql.functions import lit
df_with_flag = df.withColumn("merge_flag", lit(False))
display(df_with_flag)

# COMMAND ----------

df_with_flag.write.option("path", "/mnt/bronzelayer/Agent").mode("append").saveAsTable("bronzelayer.Agent")

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from bronzelayer.agent

# COMMAND ----------

dbutils.fs.mv("/mnt/landing/AgentData/", "/mnt/processed/AgentData/", True)

# COMMAND ----------

from datetime import datetime

# get the current time in mm-dd-yyyy format
current_time = datetime.now().strftime('%m-%d-%Y')

# print the current time
print(current_time)

# COMMAND ----------

new_folder  = "/mnt/processed/AgentData/"+current_time

dbutils.fs.mv("/mnt/landing/AgentData/", new_folder,True)

# COMMAND ----------

# dbutils.fs.mv("/mnt/processed/AgentData/", "/mnt/landing/AgentData/", True)