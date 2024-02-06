# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE silverlayer.Policy (
# MAGIC policy_id integer,
# MAGIC policy_type string,
# MAGIC customer_id integer,
# MAGIC start_date timestamp,
# MAGIC end_date timestamp,
# MAGIC premium double,
# MAGIC coverage_amount double,
# MAGIC merged_timestamp TIMESTAMP
# MAGIC ) USING DELTA LOCATION '/mnt/silverlayer/Policy' 