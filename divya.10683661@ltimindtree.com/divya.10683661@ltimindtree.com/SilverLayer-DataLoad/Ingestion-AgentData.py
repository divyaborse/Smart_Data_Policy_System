# Databricks notebook source
df = spark.sql("select * from bronzelayer.agent where merge_flag=false")
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC remove all the rows where branch id does not exists
# MAGIC

# COMMAND ----------

df_branch = spark.sql("select * from bronzelayer.branch")

# COMMAND ----------

df_result = spark.sql("select agent.* from bronzelayer.agent inner join bronzelayer.branch on agent.branch_id = branch.branch_id where agent.merge_flag = false")

# COMMAND ----------

display(df_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ensure all the phone have valid 10 digit phone no
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import length,col
df_phone = df_result.filter(length(col("agent_phone")) == 10)

# COMMAND ----------

display(df_phone)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC replace all the null email with 'admin@azurelib.com

# COMMAND ----------

df_email = df_phone.fillna({'agent_email':'admin@azurelib.com'})

# COMMAND ----------

display(df_email)

# COMMAND ----------

df_e = df_email.filter(col("agent_email") == 'admin@azurelib.com')
display(df_e)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronzelayer.agent where agent_email =''

# COMMAND ----------

df_phone.createOrReplaceTempView("agent_temp")

# COMMAND ----------

df_email = spark.sql("select a.agent_id, a.agent_name, a.agent_phone, a.branch_id,a.create_timestamp, regexp_replace(a.agent_email, '', 'admin@azurelib.com') as agent_email from agent_temp a where a.agent_email ='' ")
display(df_email)


# COMMAND ----------

# df_email = spark.sql("UPDATE agent_temp  SET agent_email = CASE WHEN agent_email = '' OR agent_email IS NULL THEN 'admin@azurelib.com' ELSE agent_email END;")
# display(df_email)

# COMMAND ----------

display(df_email)

# COMMAND ----------

replacement_character = "admin@azurelib.com"
df_email = df_phone.withColumn('agent_email', col('agent_email').replace("", replacement_character))
display(df_phone)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace

replacement_character = "admin@azurelib.com"
df_email = df_phone.withColumn('agent_email', regexp_replace(col('agent_email'), '^$', replacement_character))
display(df_email)

# COMMAND ----------

df = spark.sql("SELECT a.* FROM bronzelayer.agent a INNER JOIN bronzelayer.branch b on  a.branch_id = b.branch_id where a.merge_flag = FALSE and length(a.agent_phone)=10")
df.createOrReplaceTempView("agent_temp")
df_email = spark.sql("select a.agent_id, a.agent_name, a.agent_phone, a.branch_id,a.create_timestamp, regexp_replace(a.agent_email, '', 'admin@azurelib.com') as agent_email from agent_temp a where a.agent_email =''  UNION  select  a.agent_id, a.agent_name, a.agent_phone, a.branch_id,a.create_timestamp, agent_email from agent_temp a where a.agent_email !='' ")

display(df_email)

# COMMAND ----------

# MAGIC %md
# MAGIC add meerged date time stamp
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df_final = df_email.withColumn("merged_timestamp", current_timestamp())
display(df_final)

# COMMAND ----------

df_email.createOrReplaceTempView("clean_agent")
spark.sql(" MERGE INTO silverlayer.agent AS T USING clean_agent AS S ON  t.agent_id = s.agent_id  WHEN MATCHED THEN UPDATE SET t.agent_phone = s.agent_phone, t.agent_email = s.agent_email, t.agent_name = s.agent_name, t.branch_id= s.branch_id, t.create_Timestamp = s.create_TimeStamp, T.merged_timestamp  =  current_timestamp() When not matched then INSert (agent_phone, agent_email , agent_name, branch_id ,create_Timestamp , merged_timestamp, agent_id) values (s.agent_phone, s.agent_email , s.agent_name, s.branch_id ,s.create_Timestamp , current_timestamp(), s.agent_id)")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.agent

# COMMAND ----------

spark.sql("update bronzelayer.agent set merge_flag = True where merge_flag=False")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronzelayer.agent