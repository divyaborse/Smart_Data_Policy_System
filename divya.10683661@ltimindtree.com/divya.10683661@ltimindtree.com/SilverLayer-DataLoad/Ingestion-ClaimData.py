# Databricks notebook source
# MAGIC %md
# MAGIC Remove all where claim_id, policy_id is ,claim status,claim_amount,lastupdated null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronzelayer.claim where claim_id is not null and policy_id is not null and claim_status is not null and claim_amount is not null and LastUpdatedTimeStamp is not null and merge_flag =False

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronzelayer.claim

# COMMAND ----------

df = spark.sql("select C.* from bronzelayer.claim C inner join bronzelayer.policy P on C.policy_id = P.policy_id where C.claim_id is not null and C.policy_id is not null and C.claim_status is not null and C.claim_amount is not null and C.LastUpdatedTimeStamp is not null and C.merge_flag =False")
display(df)

# COMMAND ----------

df = spark.sql("select C.claim_id,C.policy_id,C.claim_status,C.claim_status, C.claim_amount,C.LastUpdatedTimeStamp, to_date(date_format(C.date_of_claim,'MM-dd-yyyy'),'MM-dd-yyyy') as date_of_claim  from bronzelayer.claim C inner join bronzelayer.policy P on C.policy_id = P.policy_id where C.claim_id is not null and C.policy_id is not null and C.claim_status is not null and C.claim_amount is not null and C.LastUpdatedTimeStamp is not null and C.merge_flag =False and claim_amount > 0")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.claim

# COMMAND ----------

df.createOrReplaceTempView("clean_claim")


# COMMAND ----------

# MAGIC %sql
# MAGIC merge into silverlayer.claim as T using clean_claim as S on T.claim_id = S.claim_id WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC t.claim_amount = s.claim_amount,
# MAGIC  t.LastUpdatedTimeStamp = s.LastUpdatedTimeStamp,
# MAGIC  t.policy_id = s.policy_id,
# MAGIC t.claim_status = s.claim_status,
# MAGIC t.date_of_claim = s.date_of_claim,
# MAGIC t.merged_timestamp = current_timestamp()
# MAGIC WHEN not MATCHED THEN
# MAGIC  INSERT (claim_id,claim_amount, LastUpdatedTimeStamp, policy_id, claim_status, date_of_claim,merged_timestamp ) VALUES (s.claim_id, s.claim_amount, s.LastUpdatedTimeStamp, s.policy_id, s.claim_status, s.date_of_claim,current_timestamp())

# COMMAND ----------

spark.sql(" UPDATE bronzelayer.claim set merge_flag = TRUE where merge_flag = FALSE")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronzelayer.claim