# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Silverlayer.Claim (
# MAGIC claim_id integer,
# MAGIC policy_id integer,
# MAGIC date_of_claim DATE,
# MAGIC claim_amount decimal(18,0),
# MAGIC claim_status string,
# MAGIC LastUpdatedTimeStamp timestamp,
# MAGIC merged_timestamp timestamp
# MAGIC ) USING DELTA LOCATION '/mnt/silverlayer/Claim'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  Silverlayer.Claim