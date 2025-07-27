# Databricks notebook source
display(dbutils.secrets.list('Key-Vault-DataBricksSPSecret'))

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="Key-Vault-DataBricksSPSecret",key="databricksSpSecret")

spark.conf.set("fs.azure.account.auth.type.aerooptimizesg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.aerooptimizesg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.aerooptimizesg.dfs.core.windows.net", "1c0ed18f-fef9-4706-bb75-eba21d0542c4")
spark.conf.set("fs.azure.account.oauth2.client.secret.aerooptimizesg.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.aerooptimizesg.dfs.core.windows.net", "https://login.microsoftonline.com/9b2c18e1-54fb-4641-a931-e1b8581e8e81/oauth2/token")


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import col, regexp_replace, to_timestamp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@aerooptimizesg.dfs.core.windows.net/"))

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC Storing the raw data inthe conformance container after merging the files **

# COMMAND ----------

# MAGIC %md
# MAGIC Loading the merged files into conformance container-Arrival,and also validates the data by  total rows in a file to the total rows in a dataframe(df_arrival_raw) 

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

# Initialize Spark session
spark = SparkSession.builder.appName("Load, Verify, and Save JSON Files").getOrCreate()

# Directory path in Azure Data Lake Storage (ADLS)
directory_path = "abfss://raw@aerooptimizesg.dfs.core.windows.net/AVIATIONEDGE-API/type=arrival/"

# List all files in the directory
files = dbutils.fs.ls(directory_path)
file_count = len(files)
print(f"Number of files in directory: {file_count}")

# Initialize an empty DataFrame
df_arrival_raw = None
total_rows_files = 0

# Load all files and count rows
for file in files:
    file_path = file.path  # Get the full file path
    print(f"Loading file: {file_path}")
    df = spark.read.format("json").load(file_path)
    row_count = df.count()
    print(f"Rows in file {file.name}: {row_count}")
    total_rows_files += row_count
    
    # Union DataFrames
    if df_arrival_raw is None:
        df_arrival_raw = df
    else:
        df_arrival_raw = df_arrival_raw.union(df)

# Verify total row count
print(f"Total rows across all files: {total_rows_files}")

# Count rows in the final DataFrame
df_total_rows = df_arrival_raw.count()
print(f"Total rows in DataFrame: {df_total_rows}")

# Compare the counts
if total_rows_files == df_total_rows:
    print("The total row count matches between the files and the DataFrame.")
else:
    print(f"Row count mismatch: {total_rows_files} (files) vs {df_total_rows} (DataFrame)")

# Display the schema to verify structure
df_arrival_raw.printSchema()

# Show a few rows to verify content
df_arrival_raw.show(5)

# Check for missing data in columns
missing_data = df_arrival_raw.select([count(when(col(c).isNull(), c)).alias(c) for c in df_arrival_raw.columns])
missing_data.show()

# Define the output path in the conformance container
output_path = "abfss://conformance-arrival@aerooptimizesg.dfs.core.windows.net/AviationEdge-API/arrival-merged-rawdata"

# Save the DataFrame as Delta format
df_arrival_raw.write.format("delta").mode("overwrite").save(output_path)

print(f"DataFrame saved successfully at: {output_path}")

'''
# Verify the saved files
saved_files = dbutils.fs.ls(output_path)
print("Files saved in the Delta format:")
for file in saved_files:
    print(file.name)
'''

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating the FactTable called flight and storing to in standardized enrich container ******

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_timestamp, lit

# Initialize Spark session
spark = SparkSession.builder.appName("AviationEdge Data Transformation").getOrCreate()

# Load the Delta table
df1 = spark.read.format("delta").load("abfss://conformance-arrival@aerooptimizesg.dfs.core.windows.net/AviationEdge-API/arrival-merged-rawdata")

# Select and transform columns
df_flight = df1.select(
    col("codeshared.airline.iatacode").alias("codeshared_airline_iatacode"),
    col("codeshared.flight.iataNumber").alias("codeshared_flight_iatanumber"),
    col("flight.iataNumber").alias("flight_iatanumber"),
    regexp_replace("arrival.actualTime", "t", "T").alias("arrival_actualtime"),
    regexp_replace("arrival.estimatedTime", "t", "T").alias("arrival_estimatedtime"),
    regexp_replace("arrival.scheduledTime", "t", "T").alias("arrival_scheduledttime"),
    col("arrival.gate").alias("arrival_gate"),
    col("arrival.iataCode").alias("arrivalAirport_iatacode"),
    col("arrival.estimatedRunway").alias("arrival_estimatedrunway"),
    col("arrival.actualRunway").alias("arrival_actualrunway"),
    col("arrival.delay").alias("arrival_delay")
).where((col("status") == "landed") & (col("type") == "arrival"))

# Convert time columns to timestamp
df_flight = df_flight.withColumn("arrival_actualTime", to_timestamp("arrival_actualtime", "yyyy-MM-dd'T'HH:mm:ss.SSS"))
df_flight = df_flight.withColumn("arrival_estimatedTime", to_timestamp("arrival_estimatedtime", "yyyy-MM-dd'T'HH:mm:ss.SSS"))
df_flight = df_flight.withColumn("arrival_scheduledTime", to_timestamp("arrival_scheduledttime", "yyyy-MM-dd'T'HH:mm:ss.SSS"))

# Drop unnecessary column
df_flight = df_flight.drop("arrival_scheduledttime")

# Add a new column
df_flight = df_flight.withColumn("type", lit("arrival"))

# Show the DataFrame
df_flight.show(3, truncate=False)
tp=df_flight.select("arrival_actualTime", "arrival_estimatedTime", "arrival_scheduledTime")
tp.show(3,truncate=False)

# Store the data into the standardized-enriched container
df_flight.write.format("delta").mode("overwrite").save("abfss://standardized-aviationedgeapi@aerooptimizesg.dfs.core.windows.net/AviationEdge-Arrival/arrivalflight_Fact")


# COMMAND ----------

"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_timestamp, lit

# Initialize Spark session
spark = SparkSession.builder.appName("AviationEdge Data Transformation").getOrCreate()

# Load the Delta table
df1 = spark.read.format("delta").load("abfss://conformance-arrival@aerooptimizesg.dfs.core.windows.net/AviationEdge-API/arrival-merged-rawdata")

# Select and transform columns
df_flight = df1.select(
    col("codeshared.airline.iatacode").alias("codeshared_airline_iatacode"),
    col("codeshared.flight.iataNumber").alias("codeshared_flight_iatanumber"),
    col("flight.iataNumber").alias("flight_iatanumber"),
    regexp_replace("arrival.actualTime", "t", "T").alias("arrival_actualtime"),
    regexp_replace("arrival.estimatedTime", "t", "T").alias("arrival_estimatedtime"),
    regexp_replace("arrival.scheduledTime", "t", "T").alias("arrival_scheduledttime"),
    col("arrival.gate").alias("arrival_gate"),
    col("arrival.iataCode").alias("arrivalAirport_iatacode"),
    col("arrival.estimatedRunway").alias("arrival_estimatedrunway"),
    col("arrival.actualRunway").alias("arrival_actualrunway"),
    col("arrival.delay").alias("arrival_delay")
).where((col("status") == "landed") & (col("type") == "arrival"))

# Convert time columns to timestamp
df_flight = df_flight.withColumn("arrival_actualTime", to_timestamp("arrival_actualtime", "yyyy-MM-dd'T'HH:mm:ss.SSS"))
df_flight = df_flight.withColumn("arrival_estimatedTime", to_timestamp("arrival_estimatedtime", "yyyy-MM-dd'T'HH:mm:ss.SSS"))
df_flight = df_flight.withColumn("arrival_scheduledTime", to_timestamp("arrival_scheduledttime", "yyyy-MM-dd'T'HH:mm:ss.SSS"))

# Drop unnecessary column
df_flight = df_flight.drop("arrival_scheduledttime")

# Add a new column
df_flight = df_flight.withColumn("type", lit("arrival"))

# Show the DataFrame (without assignment)
df_flight.show(3, truncate=False)

# Storing the data into the standardized-enriched container
df_flight.write.format("delta").mode("overwrite").save("abfss://standardized-aviationedgeapi@aerooptimizesg.dfs.core.windows.net/AviationEdge-API/arrivalflight_Fact")
"""


# COMMAND ----------


"""
df_flight=df_arrival_raw.select(
    col("codeshared.airline.iatacode").alias("airline_iatacode"),
    col("codeshared.flight.iataNumber").alias("flight_iatanumber"),
    regexp_replace("arrival.actualTime","t","T").alias("arrival_actualtime"),
    regexp_replace("arrival.estimatedTime","t","T").alias("arrival_estimatedtime"),
    regexp_replace("arrival.scheduledTime","t","T").alias("arrival_scheduledttime"),
    col("arrival.gate").alias("arrival_gate"),
    col("arrival.iataCode").alias("arrivalAirport_iatacode"),
    col("arrival.estimatedRunway").alias("arrival_estimatedrunway"),
    col("arrival.actualRunway").alias("arrival_actualrunway"),
    col("arrival.delay").alias("arrival_delay")
    
).where(col("status")=="landed")
"""

# COMMAND ----------

# MAGIC %md
# MAGIC > **Airline dimension**

# COMMAND ----------



df1 = spark.read.format("delta").load("abfss://conformance-arrival@aerooptimizesg.dfs.core.windows.net/AviationEdge-API/arrival-merged-rawdata")
df_airline=df_arrival_raw.select(

    col("airline.iataCode").alias("airline_iataCode"),
    col("airline.icaoCode").alias("airline_icaoCode"),
    col("airline.name").alias("airline_name")
    
)
tp=df_airline.distinct()
tp.show()
df_airline.write.mode("overwrite").format("delta").save("abfss://standardized-aviationedgeapi@aerooptimizesg.dfs.core.windows.net/AviationEdge-Arrival/airline_info_Dim/")


# COMMAND ----------

# MAGIC %md
# MAGIC > **AirPortDimension table stored in standardized -avviation edge APi**

# COMMAND ----------


df1 = spark.read.format("delta").load("abfss://conformance-arrival@aerooptimizesg.dfs.core.windows.net/AviationEdge-API/arrival-merged-rawdata")
df_airport_temp=df1.select(
    col("arrival.iataCode").alias("arrival_airport_iataCode"),
    col("arrival.icaoCode").alias("arrival_airport_icaoCode"),
    col("arrival.estimatedRunway").alias("arrival_Runway"),
    col("arrival.gate").alias("arrival_gate"),
    col("arrival.terminal").alias("arrival_terminal")
    )
df_airport=df_airport_temp.dropna()


df_airport.write.mode("overwrite").format("delta").save("abfss://standardized-aviationedgeapi@aerooptimizesg.dfs.core.windows.net/AviationEdge-Arrival/airport_info_Dim/")


# COMMAND ----------

# MAGIC %md
# MAGIC > **Flight Dimension** 

# COMMAND ----------

    df1 = spark.read.format("delta").load("abfss://conformance-arrival@aerooptimizesg.dfs.core.windows.net/AviationEdge-API/arrival-merged-rawdata")

df2=df1.select(
    col("codeshared.flight.iataNumber").alias("flight_iatanumber"),
    col("codeshared.flight.icaoNumber").alias("flight_icaoNumber"),
    col("codeshared.flight.number").alias("flight_number"),
    col("codeshared.airline.iataCode").alias("airline_iataCode"),
)

df_flight=df2.dropna()
tp=df_flight.distinct()
tp.show()
df_flight.write.mode("overwrite").format("delta").save("abfss://standardized-aviationedgeapi@aerooptimizesg.dfs.core.windows.net/AviationEdge-Arrival/flight_info_Dim/")