# Databricks notebook source
# MAGIC %md
# MAGIC Remove all where brnach_id not null

# COMMAND ----------

df= spark.sql("select * from bronzelayer.branch where branch_id is not null and merge_flag=False")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Remove all the leading and trailing spaces in Brnach Country and covert it into 

# COMMAND ----------

df = spark.sql("select b.branch_id,b.branch_city, upper(trim(b.branch_country)) as branch_country  from bronzelayer.branch b  where branch_id is not null and merge_flag = false")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Merge into Silver layer table
# MAGIC

# COMMAND ----------

df.createOrReplaceTempView("clean_branch")

# COMMAND ----------

spark.sql("select * from silverlayer.branch")

# COMMAND ----------

spark.sql( "MERGE INTO silverlayer.branch AS T USING clean_branch AS S ON t.branch_id = s.branch_id when MATCHED THEN UPDATE SET t.branch_country = s.branch_country , t.branch_city = s.branch_city, t.merged_timestamp = current_timestamp()  when NOT MATCHED THEN insert (branch_id, branch_country,branch_city,merged_timestamp ) values (s.branch_id, s.branch_country , s.branch_city,current_timestamp())")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.branch

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Update the merged_flag in the bronzelayer table

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE bronzelayer.branch set merge_flag = true where merge_flag = FALSE

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronzelayer.branch

# COMMAND ----------

