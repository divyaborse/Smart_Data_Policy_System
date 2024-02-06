# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace table silverlayer.Agent(
# MAGIC agent_id integer,
# MAGIC agent_name string, 
# MAGIC agent_email string,
# MAGIC agent_phone string, 
# MAGIC branch_id integer, 
# MAGIC create_timestamp timestamp,
# MAGIC merged_timestamp TIMESTAMP
# MAGIC  ) using delta location '/mnt/silverlayer/Agent'

# COMMAND ----------

# MAGIC %sql
# MAGIC create database silverlayer

# COMMAND ----------

# MAGIC %sql
# MAGIC use silverlayer

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.Agent