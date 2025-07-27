# Databricks notebook source
service_credential = dbutils.secrets.get(scope="Key-Vault-DataBricksSPSecret",key="databricksSpSecret")

spark.conf.set("fs.azure.account.auth.type.aerooptimizesg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.aerooptimizesg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.aerooptimizesg.dfs.core.windows.net", "1c0ed18f-fef9-4706-bb75-eba21d0542c4")
spark.conf.set("fs.azure.account.oauth2.client.secret.aerooptimizesg.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.aerooptimizesg.dfs.core.windows.net", "https://login.microsoftonline.com/9b2c18e1-54fb-4641-a931-e1b8581e8e81/oauth2/token")


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import col, regexp_replace, to_timestamp,count
from pyspark.sql import DataFrame
from pyspark.sql import functions as F




# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@aerooptimizesg.dfs.core.windows.net/AVIATIONEDGE-API/type=departure/"))

# COMMAND ----------

directory_path="abfss://raw@aerooptimizesg.dfs.core.windows.net/AVIATIONEDGE-API/type=departure/"
files=dbutils.fs.ls(directory_path)
file_count=len(files)
print(f"Number of files in the directory: {file_count}")
flie_rowcount=0
df_departure_raw=None
for file in files:
    file_path=file.path
    df=spark.read.format("json").load(file_path)
    print(f"Number of rows in the {file.name}:{df.count()}")
    flie_rowcount+=df.count()
    
    if df_departure_raw is not None:
        df_departure_raw=df_departure_raw.union(df)
    else:
        df_departure_raw=df
print(f"Total number of rows in the directory: {flie_rowcount}")
print(f"Total number of rows in the dataframe: {df_departure_raw.count()}")

#check the missing values count
missing_data = df_departure_raw.select([count(when(col(c).isNull(), c)).alias(c) for c in df_departure_raw.columns])
missing_data.show()

if flie_rowcount == df_departure_raw.count():
    print(f"Total Count of Rows in the directory:{flie_rowcount} and dataframe:{df_departure_raw.count()} are same")
else:
    print(f"Total Count of Rows in the directory:{flie_rowcount} and dataframe:{df_departure_raw.count()} are not same")

df_departure_raw.write.mode("overwrite").format("delta").save("abfss://conformance-departure@aerooptimizesg.dfs.core.windows.net/AviationEdge/departure-merged-rawdata/")

df_departure_raw.show(3,truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC FactTable=departureFlight-Fact

# COMMAND ----------

df1=spark.read.format("delta").load("abfss://conformance-departure@aerooptimizesg.dfs.core.windows.net/AviationEdge/departure-merged-rawdata/")
df_flight=df1.select(
    col("codeshared.airline.iatacode").alias("codeshared_airline_iatacode"),
    col("codeshared.flight.iataNumber").alias("codeshared_flight_iatanumber"),
    col("flight.iataNumber").alias("flight_iatanumber"),
    regexp_replace("departure.actualTime", "t", "T").alias("departure_actualtime"),
    regexp_replace("departure.estimatedTime", "t", "T").alias("departure_estimatedtime"),
    regexp_replace("departure.scheduledTime", "t", "T").alias("departure_scheduledtime"),
    col("departure.gate").alias("departure_gate"),
    col("departure.iataCode").alias("departureAirport_iatacode"),
    col("departure.estimatedRunway").alias("departure_estimatedrunway"),
    col("departure.actualRunway").alias("departure_actualrunway"),
    col("departure.delay").alias("departure_delay")
).where((col("status") == "active") & (col("type") == "departure"))
from pyspark.sql.functions import to_timestamp, col, when


df_flight = df_flight.withColumn(
    "departure_actualtime",to_timestamp(col("departure_actualtime"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))


df_flight = df_flight.withColumn(
    "departure_estimatedtime",to_timestamp(col("departure_estimatedtime"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))


df_flight = df_flight.withColumn(
    "departure_scheduledtime",to_timestamp(col("departure_scheduledtime"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))


df_flight.write.format("delta").mode("overwrite").save("abfss://standardized-aviationedgeapi@aerooptimizesg.dfs.core.windows.net/AviationEdge-Departure/departureflight-Fact/")





# COMMAND ----------

df1=spark.read.format("delta").load("abfss://conformance-departure@aerooptimizesg.dfs.core.windows.net/AviationEdge/departure-merged-rawdata/")
df_Airline=df1.select(