# Databricks notebook source
# MAGIC %md
# MAGIC This table would contain the number and amount of claims by policy type and claim status. It would be used to monitor the claims process and identify any trends or issues.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view vm_gold_claims_by_policy_type_and_status as 
# MAGIC select p.policy_type,
# MAGIC claim_status,
# MAGIC count(*) as total_claims,
# MAGIC sum(claim_amount) as total_claim_amount
# MAGIC
# MAGIC from silverlayer.claim c join silverlayer.policy p
# MAGIC on c.policy_id = p.policy_id
# MAGIC group by
# MAGIC p.policy_type,
# MAGIC claim_status having p.policy_type is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vm_gold_claims_by_policy_type_and_status

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into goldlayer.claims_by_policy_type_and_status as T using vm_gold_claims_by_policy_type_and_status as S on T.policy_type = S.policy_type
# MAGIC and T.claim_status = S.claim_status when matched then 
# MAGIC update set
# MAGIC T.total_claim_amount = S.total_claim_amount,T.total_claims = S.total_claims,
# MAGIC T.updated_timestamp = current_timestamp()
# MAGIC when not matched then
# MAGIC insert 
# MAGIC (policy_type ,claim_status,total_claim_amount,total_claims,updated_timestamp)
# MAGIC values
# MAGIC (
# MAGIC
# MAGIC   s.policy_type,
# MAGIC   s.claim_status,
# MAGIC   s.total_claim_amount,
# MAGIC   s.total_claims,
# MAGIC current_timestamp()
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from goldlayer.claims_by_policy_type_and_status 

# COMMAND ----------

# MAGIC %md
# MAGIC  Analyze the claim data based on the policy type like AVG, MAX, MIN, Count of claim.

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view vm_gold_claims_analysis as
# MAGIC select 
# MAGIC policy_type,
# MAGIC avg(claim_amount) as avg_claim_amount,
# MAGIC max(claim_amount) as max_claim_amount,
# MAGIC min(claim_amount) as min_claim_amount,
# MAGIC count(distinct claim_id) as total_claims
# MAGIC
# MAGIC from 
# MAGIC silverlayer.claim c join silverlayer.policy p on c.policy_id = p.policy_id
# MAGIC group by 
# MAGIC p.policy_type having p.policy_type is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vm_gold_claims_analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into goldlayer.claims_analysis
# MAGIC as T using vm_gold_claims_analysis as S on T.policy_type = S.policy_type
# MAGIC when matched then
# MAGIC update set
# MAGIC T.avg_claim_amount = S.avg_claim_amount,
# MAGIC T.max_claim_amount = S.max_claim_amount,
# MAGIC T.min_claim_amount = S.min_claim_amount,
# MAGIC T.total_claims = S.total_claims,
# MAGIC T.updated_Timestamp = current_timestamp()
# MAGIC when not matched then
# MAGIC insert
# MAGIC ( policy_type,avg_claim_amount,max_claim_amount,min_claim_amount,total_claims,updated_Timestamp)
# MAGIC
# MAGIC values
# MAGIC ( S.policy_type,S.avg_claim_amount,S.max_claim_amount,S.min_claim_amount,S.total_claims,current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from goldlayer.claims_analysis