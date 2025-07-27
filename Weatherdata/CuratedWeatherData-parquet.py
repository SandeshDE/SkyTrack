# Databricks notebook source
# MAGIC %md
# MAGIC JDBC Properties to connect the AZure DB Residing in SSMS

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="Key-Vault-DataBricksSPSecret",key="databricksSpSecret")

spark.conf.set("fs.azure.account.auth.type.aerooptimizesg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.aerooptimizesg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.aerooptimizesg.dfs.core.windows.net", "1c0ed18f-fef9-4706-bb75-eba21d0542c4")
spark.conf.set("fs.azure.account.oauth2.client.secret.aerooptimizesg.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.aerooptimizesg.dfs.core.windows.net", "https://login.microsoftonline.com/9b2c18e1-54fb-4641-a931-e1b8581e8e81/oauth2/token")

# COMMAND ----------

jdbcHostname = "testdbse.database.windows.net"
jdbcPort = "1433"
jdbcDatabase = "testdb"
jdbcUsername = "testdb@testdbse"  
jdbcPassword = "Test@123" 

# JDBC URL without user and password in it
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# Set up the connection properties with user and password
connectionProperties = {
    "user": jdbcUsername,
    "password": jdbcPassword,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}



# COMMAND ----------

(dbutils.fs.ls("abfss://standardized-weatherdata@aerooptimizesg.dfs.core.windows.net"))

# COMMAND ----------

# MAGIC %md
# MAGIC converting to Parquetfiles and moving to Curatedlayer

# COMMAND ----------


files=dbutils.fs.ls("abfss://standardized-weatherdata@aerooptimizesg.dfs.core.windows.net")
save_path="abfss://curated-weather@aerooptimizesg.dfs.core.windows.net/"
for file in files:
  save_path="abfss://curated-weather@aerooptimizesg.dfs.core.windows.net/"+file.name
  
  df=spark.read.format("delta").load(file.path)
  df=df.withColumn("date",df["date"].cast("date"))
  df.show(3,truncate=False)
  df.coalesce(1).write.format("parquet").save(save_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Moving the data to AzureDB

# COMMAND ----------

paths="abfss://curated-weather@aerooptimizesg.dfs.core.windows.net/DFW-MIA"
df=spark.read.format("parquet").load(paths)
jdbcHostname = "testdbse.database.windows.net"
jdbcPort = "1433"
jdbcDatabase = "testdb"
jdbcUsername = "testdb@testdbse"  
jdbcPassword = "Test@123" 

# JDBC URL without user and password in it
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# Set up the connection properties with user and password
connectionProperties = {
    "user": jdbcUsername,
    "password": jdbcPassword,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Specify the table name
table_name = "DFW_MIA_WEATHER_DATA"

# Write to SQL Server table
df.write.mode("append").jdbc(url=jdbcUrl, table=table_name, properties=connectionProperties)


   

# COMMAND ----------

paths="abfss://curated-weather@aerooptimizesg.dfs.core.windows.net/LAX-SFO"
df=spark.read.format("parquet").load(paths)
jdbcHostname = "testdbse.database.windows.net"
jdbcPort = "1433"
jdbcDatabase = "testdb"
jdbcUsername = "testdb@testdbse"  
jdbcPassword = "Test@123" 

# JDBC URL without user and password in it
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# Set up the connection properties with user and password
connectionProperties = {
    "user": jdbcUsername,
    "password": jdbcPassword,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Specify the table name
table_name = "LAX_SFO_WEATHER_DATA"

# Write to SQL Server table
df.write.mode("append").jdbc(url=jdbcUrl, table=table_name, properties=connectionProperties)

# COMMAND ----------

paths="abfss://curated-weather@aerooptimizesg.dfs.core.windows.net/JFk-LAX"
df=spark.read.format("parquet").load(paths)
jdbcHostname = "testdbse.database.windows.net"
jdbcPort = "1433"
jdbcDatabase = "testdb"
jdbcUsername = "testdb@testdbse"  
jdbcPassword = "Test@123" 

# JDBC URL without user and password in it
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# Set up the connection properties with user and password
connectionProperties = {
    "user": jdbcUsername,
    "password": jdbcPassword,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Specify the table name
table_name = "JFK_LAX_WEATHER_DATA"

# Write to SQL Server table
df.write.mode("append").jdbc(url=jdbcUrl, table=table_name, properties=connectionProperties)


# COMMAND ----------

paths="abfss://curated-weather@aerooptimizesg.dfs.core.windows.net/ORD-LAX"
df=spark.read.format("parquet").load(paths)
jdbcHostname = "testdbse.database.windows.net"
jdbcPort = "1433"
jdbcDatabase = "testdb"
jdbcUsername = "testdb@testdbse"  
jdbcPassword = "Test@123" 

# JDBC URL without user and password in it
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# Set up the connection properties with user and password
connectionProperties = {
    "user": jdbcUsername,
    "password": jdbcPassword,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Specify the table name
table_name = "ORD_LAX_WEATHER_DATA"

# Write to SQL Server table
df.write.mode("append").jdbc(url=jdbcUrl, table=table_name, properties=connectionProperties)