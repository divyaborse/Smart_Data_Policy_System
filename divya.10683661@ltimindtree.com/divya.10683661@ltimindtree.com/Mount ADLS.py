# Databricks notebook source
dbutils.fs.mount( source = 'wasbs://bronzelayer@projectpolicysystem.blob.core.windows.net', 
                 mount_point= '/mnt/bronzelayer', extra_configs ={'fs.azure.sas.bronzelayer.projectpolicysystem.blob.core.windows.net':'?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-02-11T23:34:24Z&st=2024-02-03T15:34:24Z&spr=https&sig=CitFVZw%2FX%2FUnW7zo3z0TSJtY6mK5GcfImBCYLE0gERM%3D'})
print("mounting completed")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/bronzelayer

# COMMAND ----------

# MAGIC %md
# MAGIC Mount landing container
# MAGIC

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://landing@projectpolicysystem.blob.core.windows.net', 
                 mount_point= '/mnt/landing', extra_configs ={'fs.azure.sas.landing.projectpolicysystem.blob.core.windows.net':'?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-02-11T23:34:24Z&st=2024-02-03T15:34:24Z&spr=https&sig=CitFVZw%2FX%2FUnW7zo3z0TSJtY6mK5GcfImBCYLE0gERM%3D'})
print("mounting completed")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/landing

# COMMAND ----------

# MAGIC %md 
# MAGIC mount processed container
# MAGIC

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://processed@projectpolicysystem.blob.core.windows.net', 
                 mount_point= '/mnt/processed', extra_configs ={'fs.azure.sas.processed.projectpolicysystem.blob.core.windows.net':'?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-02-11T23:34:24Z&st=2024-02-03T15:34:24Z&spr=https&sig=CitFVZw%2FX%2FUnW7zo3z0TSJtY6mK5GcfImBCYLE0gERM%3D'})
print("mounting completed")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/processed

# COMMAND ----------

# MAGIC %md
# MAGIC mount silverlayer container
# MAGIC

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://silverlayer@projectpolicysystem.blob.core.windows.net', 
                 mount_point= '/mnt/silverlayer', extra_configs ={'fs.azure.sas.silverlayer.projectpolicysystem.blob.core.windows.net':'?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-02-11T23:34:24Z&st=2024-02-03T15:34:24Z&spr=https&sig=CitFVZw%2FX%2FUnW7zo3z0TSJtY6mK5GcfImBCYLE0gERM%3D'})


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/silverlayer

# COMMAND ----------

# MAGIC %md
# MAGIC mount gold layer container
# MAGIC

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://goldlayer@projectpolicysystem.blob.core.windows.net', 
                 mount_point= '/mnt/goldlayer', extra_configs ={'fs.azure.sas.goldlayer.projectpolicysystem.blob.core.windows.net':'?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-02-11T23:34:24Z&st=2024-02-03T15:34:24Z&spr=https&sig=CitFVZw%2FX%2FUnW7zo3z0TSJtY6mK5GcfImBCYLE0gERM%3D'})

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/goldlayer