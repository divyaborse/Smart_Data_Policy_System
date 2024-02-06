# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE table silverlayer.Branch(
# MAGIC branch_id INT,
# MAGIC branch_country string,
# MAGIC branch_city string,
# MAGIC merged_timestamp TIMESTAMP)using DELTA location '/mnt/silverlayer/Branch'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silverlayer.branch