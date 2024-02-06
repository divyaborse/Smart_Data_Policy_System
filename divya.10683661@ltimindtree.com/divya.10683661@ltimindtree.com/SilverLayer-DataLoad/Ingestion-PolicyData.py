# Databricks notebook source
# MAGIC %sql
# MAGIC select * from bronzelayer.policy

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronzelayer.policy where customer_id is not null and policy_id is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC select p.* from bronzelayer.policy as p inner join bronzelayer.customer as c on p.customer_id =c.customer_id where p.customer_id is not null and p.policy_id is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC select p.* from bronzelayer.policy as p inner join bronzelayer.customer as c on p.customer_id =c.customer_id where p.customer_id is not null and p.policy_id is not null and premium > 0 and coverage_amount > 0 

# COMMAND ----------

# df =spark.sql("select 
#               p.* from bronzelayer.policy as p inner
#                join bronzelayer.customer
#                as c
#                on p.customer_id =c.customer_id 
#               where 
#               p.customer_id is not null and p.policy_id is not null and p.premium > 0 and p.coverage_amount > 0  and 
# p.end_date > p.start_date")

# COMMAND ----------

df = spark.sql("""
    SELECT p.*
    FROM bronzelayer.policy AS p
    INNER JOIN bronzelayer.customer AS c ON p.customer_id = c.customer_id
    WHERE p.customer_id IS NOT NULL
    AND p.policy_id IS NOT NULL
    AND p.premium > 0
    AND p.coverage_amount > 0  
    AND p.end_date > p.start_date
    AND p.merge_flag=False
""")

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView("clean_policy")

# COMMAND ----------

# MAGIC %sql
# MAGIC Merge into  silverlayer.policy AS T USING clean_policy AS S ON t.policy_id = s.policy_id WHEN MATCHED THEN UPDATE SET T.policy_type = s.policy_type , T.premium = s.premium, T.end_date = s.end_date, T.start_date = s.start_date, T.coverage_amount =s.coverage_amount, T.customer_id = s.customer_id, T.merged_timestamp = current_timestamp()  WHEN NOT MATCHED THEN INSERT (policy_id, policy_type,premium,end_date, start_date,coverage_amount,customer_id,merged_timestamp ) VALUES  (s.policy_id, s.policy_type,s.premium,s.end_date, s.start_date,s.coverage_amount,s.customer_id,current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.policy

# COMMAND ----------

# MAGIC %sql
# MAGIC update bronzelayer.policy set merge_flag =True where merge_flag=False

# COMMAND ----------

