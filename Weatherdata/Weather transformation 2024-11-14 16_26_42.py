# Databricks notebook source
service_credential = dbutils.secrets.get(scope="Key-Vault-DataBricksSPSecret",key="databricksSpSecret")

spark.conf.set("fs.azure.account.auth.type.aerooptimizesg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.aerooptimizesg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.aerooptimizesg.dfs.core.windows.net", "1c0ed18f-fef9-4706-bb75-eba21d0542c4")
spark.conf.set("fs.azure.account.oauth2.client.secret.aerooptimizesg.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.aerooptimizesg.dfs.core.windows.net", "https://login.microsoftonline.com/9b2c18e1-54fb-4641-a931-e1b8581e8e81/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://raw-weatherdata@aerooptimizesg.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %md
# MAGIC standardized-weather container containing the JFK-LAX

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import col, regexp_replace, to_timestamp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
files=dbutils.fs.ls("abfss://raw-weatherdata@aerooptimizesg.dfs.core.windows.net/JFk-LAX")
final_df=None
for file in files:
  df = spark.read.format("json").load(file.path)

  display(df.show(3,truncate=False))
  
  if final_df is None:
    print(file.name)
    final_df=df
    
    display(final_df.show(3,truncate=False))
  else:
    print(file.name)
    final_df=final_df.union(df)

final_df.write.mode("overwrite").format("delta").save("abfss://conformance-weather@aerooptimizesg.dfs.core.windows.net/JFk-LAX")


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import col, regexp_replace, to_timestamp
from pyspark.sql import DataFrame
tmp=spark.read.format("delta").load("abfss://conformance-weather@aerooptimizesg.dfs.core.windows.net/JFk-LAX") 

from pyspark.sql.functions import col, explode


df = tmp.withColumn("element", explode(col("data"))).select(
    col("element.date").alias("date"),
    col("element.prcp").alias("avg_precipitation_mm"),
    col("element.pres").alias("avg_sealevel_airpressure"),
    col("element.tavg").alias("avg_temperature"),
    col("element.tmax").alias("max_temperature"),
    col("element.tmin").alias("min_temperature"),
    col("element.wspd").alias("avg_wind_speed")
)

df.write.mode("overwrite").format("delta").save("abfss://standardized-weatherdata@aerooptimizesg.dfs.core.windows.net/JFk-LAX")



# COMMAND ----------

# MAGIC %md
# MAGIC standardized-weather,COnformance container containing the DFW-MIA

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import col, regexp_replace, to_timestamp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
files=dbutils.fs.ls("abfss://raw-weatherdata@aerooptimizesg.dfs.core.windows.net/DFW-MIA")
final_df=None
for file in files:
  df = spark.read.format("json").load(file.path)

  display(df.show(3,truncate=False))
  
  if final_df is None:
    print(file.name)
    final_df=df
    
    display(final_df.show(3,truncate=False))
  else:
    print(file.name)
    final_df=final_df.union(df)

final_df.write.mode("overwrite").format("delta").save("abfss://conformance-weather@aerooptimizesg.dfs.core.windows.net/DFW-MIA")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import col, regexp_replace, to_timestamp
from pyspark.sql import DataFrame
tmp=spark.read.format("delta").load("abfss://conformance-weather@aerooptimizesg.dfs.core.windows.net/DFW-MIA") 

from pyspark.sql.functions import col, explode


df = tmp.withColumn("element", explode(col("data"))).select(
    col("element.date").alias("date"),
    col("element.prcp").alias("avg_precipitation_mm"),
    col("element.pres").alias("avg_sealevel_airpressure"),
    col("element.tavg").alias("avg_temperature"),
    col("element.tmax").alias("max_temperature"),
    col("element.tmin").alias("min_temperature"),
    col("element.wspd").alias("avg_wind_speed")
)

df.write.mode("overwrite").format("delta").save("abfss://standardized-weatherdata@aerooptimizesg.dfs.core.windows.net/DFW-MIA")


# COMMAND ----------

# MAGIC %md
# MAGIC standardized-weather,COnformance container containing the LAX-SFO

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import col, regexp_replace, to_timestamp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
files=dbutils.fs.ls("abfss://raw-weatherdata@aerooptimizesg.dfs.core.windows.net/LAX-SFO")
final_df=None
for file in files:
  df = spark.read.format("json").load(file.path)

  display(df.show(3,truncate=False))
  
  if final_df is None:
    print(file.name)
    final_df=df
    
    display(final_df.show(3,truncate=False))
  else:
    print(file.name)
    final_df=final_df.union(df)

final_df.write.mode("overwrite").format("delta").save("abfss://conformance-weather@aerooptimizesg.dfs.core.windows.net/LAX-SFO")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import col, regexp_replace, to_timestamp
from pyspark.sql import DataFrame
tmp=spark.read.format("delta").load("abfss://conformance-weather@aerooptimizesg.dfs.core.windows.net/LAX-SFO") 

from pyspark.sql.functions import col, explode


df = tmp.withColumn("element", explode(col("data"))).select(
    col("element.date").alias("date"),
    col("element.prcp").alias("avg_precipitation_mm"),
    col("element.pres").alias("avg_sealevel_airpressure"),
    col("element.tavg").alias("avg_temperature"),
    col("element.tmax").alias("max_temperature"),
    col("element.tmin").alias("min_temperature"),
    col("element.wspd").alias("avg_wind_speed")
)

df.write.mode("overwrite").format("delta").save("abfss://standardized-weatherdata@aerooptimizesg.dfs.core.windows.net/LAX-SFO")

# COMMAND ----------

# MAGIC %md
# MAGIC standardized-weather,COnformance container containing the ORD-LAX

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import col, regexp_replace, to_timestamp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
files=dbutils.fs.ls("abfss://raw-weatherdata@aerooptimizesg.dfs.core.windows.net/ORD-LAX")
final_df=None
for file in files:
  df = spark.read.format("json").load(file.path)

  display(df.show(3,truncate=False))
  
  if final_df is None:
    print(file.name)
    final_df=df
    
    display(final_df.show(3,truncate=False))
  else:
    print(file.name)
    final_df=final_df.union(df)

final_df.write.mode("overwrite").format("delta").save("abfss://conformance-weather@aerooptimizesg.dfs.core.windows.net/ORD-LAX")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import col, regexp_replace, to_timestamp
from pyspark.sql import DataFrame
tmp=spark.read.format("delta").load("abfss://conformance-weather@aerooptimizesg.dfs.core.windows.net/ORD-LAX") 

from pyspark.sql.functions import col, explode


df = tmp.withColumn("element", explode(col("data"))).select(
    col("element.date").alias("date"),
    col("element.prcp").alias("avg_precipitation_mm"),
    col("element.pres").alias("avg_sealevel_airpressure"),
    col("element.tavg").alias("avg_temperature"),
    col("element.tmax").alias("max_temperature"),
    col("element.tmin").alias("min_temperature"),
    col("element.wspd").alias("avg_wind_speed")
)

df.write.mode("overwrite").format("delta").save("abfss://standardized-weatherdata@aerooptimizesg.dfs.core.windows.net/ORD-LAX")