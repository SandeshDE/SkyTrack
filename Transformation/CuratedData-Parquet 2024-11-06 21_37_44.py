# Databricks notebook source
# MAGIC %md
# MAGIC Load of Delata Tables of arrival and departure

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

# MAGIC %md
# MAGIC **Loading the Data as a parquet file for arrival to the curated layer**

# COMMAND ----------

directory_path="abfss://standardized-aviationedgeapi@aerooptimizesg.dfs.core.windows.net/AviationEdge-Arrival/"
files=(dbutils.fs.ls(directory_path))
for file in files:
  path=directory_path+file.name
  save_path="abfss://curatedlayer@aerooptimizesg.dfs.core.windows.net/curatedlayer/"+file.name
  df=spark.read.format("delta").load(path)
  df.coalesce(1).write.mode("overwrite").format("parquet").save(save_path)



# COMMAND ----------

directory_path="abfss://standardized-aviationedgeapi@aerooptimizesg.dfs.core.windows.net/AviationEdge-Departure/"
files=(dbutils.fs.ls(directory_path))
for file in files:
  path=directory_path+file.name
  save_path="abfss://curatedlayer@aerooptimizesg.dfs.core.windows.net/curatedlayer/"+file.name
  df=spark.read.format("delta").load(path)
  df.coalesce(1).write.mode("overwrite").format("parquet").save(save_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Loading the Data into MSSql Server

# COMMAND ----------

directory_path="abfss://curatedlayer@aerooptimizesg.dfs.core.windows.net/curatedlayer/"
f=dbutils.fs.ls(directory_path)
for files in f:
  df=spark.read.format("parquet").load(directory_path+files.name)
  print(f"table_name:{files.name}")
  df.printSchema()






# COMMAND ----------

directory_path="abfss://curatedlayer@aerooptimizesg.dfs.core.windows.net/curatedlayer/airline_info_Dim"
df=spark.read.format("parquet").load(directory_path)


# COMMAND ----------

# Azure SQL Database connection details
jdbcHostname = "testdb.database.windows.net"
jdbcPort = "1433"
jdbcDatabase = "testdb"
jdbcUsername = "testdb"
jdbcPassword = "Test@123"

# Construct the JDBC URL
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};user={jdbcUsername};password={jdbcPassword};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net"


# COMMAND ----------

directory_path = "abfss://curatedlayer@aerooptimizesg.dfs.core.windows.net/curatedlayer/airline_info_Dim"
df = spark.read.format("parquet").load(directory_path)
df.printSchema()

# COMMAND ----------

table_name="arrivalflight_Fact"
directory_path="abfss://curatedlayer@aerooptimizesg.dfs.core.windows.net/curatedlayer/airline_info_Dim"
df=spark.read.format("parquet").load(directory_path)
df.write.mode("append").jdbc(url=jdbcUrl,table=table_name,properties=)

# COMMAND ----------

directory_path = "abfss://curatedlayer@aerooptimizesg.dfs.core.windows.net/curatedlayer/airline_info_Dim"

df = spark.read.format("parquet").load(directory_path)

# COMMAND ----------

# Load existing primary keys from the SQL table
existing_keys_df = spark.read.jdbc(
    url=jdbcUrl,
    table=table_name,
    properties=connectionProperties
).select("airline_iataCode")

# Filter out records from the DataFrame that already exist in the SQL table
df_filtered = df.join(existing_keys_df, on="airline_iataCode", how="left_anti")

# Append only the non-duplicate records to the SQL table
df_filtered.write.mode("append").jdbc(url=jdbcUrl, table=table_name, properties=connectionProperties)


# COMMAND ----------

# MAGIC %md
# MAGIC Loadig the airline_info_Dim

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# Azure SQL Database connection details
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
table_name = "airline_info_Dim"

# Load data from Azure Data Lake Storage and remove duplicates
directory_path = "abfss://curatedlayer@aerooptimizesg.dfs.core.windows.net/curatedlayer/airline_info_Dim"
df = spark.read.format("parquet").load(directory_path).dropDuplicates(["airline_iataCode"])

# Load existing primary keys from the SQL table and broadcast it to optimize join performance
existing_keys_df = spark.read.jdbc(
    url=jdbcUrl,
    table=table_name,
    properties=connectionProperties
).select("airline_iataCode").distinct()

# Perform a left anti-join to exclude duplicates
df_filtered = df.join(F.broadcast(existing_keys_df), on="airline_iataCode", how="left_anti")

# Append only the non-duplicate records to the SQL table
df_filtered.write.mode("append").jdbc(url=jdbcUrl, table=table_name, properties=connectionProperties)


# COMMAND ----------

directory_path = "abfss://curatedlayer@aerooptimizesg.dfs.core.windows.net/curatedlayer/arrivalflight_Fact"
df = spark.read.format("parquet").load(directory_path)
df.show(10,truncate=False)




# COMMAND ----------

df1=spark.read.format("parquet").load("abfss://curatedlayer@aerooptimizesg.dfs.core.windows.net/curatedlayer/arrivalflight_Fact")
df2=df1.select("*").where(col("arrivalAirport_iatacode")=="jfk").dropna().distinct()
df2.show(3,truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC so to miminimze the data from 50K rows to 18K rows,we need the data that is necessary .So we applied the routes 

# COMMAND ----------

# Load the DataFrames from the specified paths
df = spark.read.format("parquet").load("abfss://curatedlayer@aerooptimizesg.dfs.core.windows.net/curatedlayer/departureflight-Fact")
df1 = spark.read.format("parquet").load("abfss://curatedlayer@aerooptimizesg.dfs.core.windows.net/curatedlayer/arrivalflight_Fact")

# Filter rows where the departure IATA code is "ORD"
df2 = df.select("*").where(col("departureAirport_iatacode") == "ord").dropna().distinct()

# Filter rows where the arrival IATA code is "JFK"
df3 = df1.select("*").where(col("arrivalAirport_iatacode") == "jfk").dropna().distinct()



# Perform the inner join on the flight number and select only columns from the departure data (df2)
df4 = df2.join(df3, on="flight_iatanumber", how="inner")
df5=df4.select(df2["*"]).where(col("departureAirport_iatacode") == col("arrivalAirport_iatacode"))
# Show the resulting DataFrame
df5.show(3, truncate=False)
display(df5.count())

# COMMAND ----------

from pyspark.sql.functions import col

# Load the DataFrames from the specified paths
df_curated_departure = spark.read.format("parquet").load("abfss://curatedlayer@aerooptimizesg.dfs.core.windows.net/curatedlayer/departureflight-Fact")
df_curated_arrival = spark.read.format("parquet").load("abfss://curatedlayer@aerooptimizesg.dfs.core.windows.net/curatedlayer/arrivalflight_Fact")

# Dictionary of departure and arrival airport codes
values = {
    "lax": "sfo",
    "jfk": "lax",
    "ord": "jfk",
    "ord": "lax",
    "atl": "mco",
    "las": "fll",
    "dfw": "mia"
}

df_reduced_departure = None

# Iterate over each dep and arr pair in the dictionary
for dep, arr in values.items():
    # Filter departure and arrival DataFrames based on dep and arr values
    df_departure_filtered = df_curated_departure.where(col("departureAirport_iatacode") == dep).dropna().distinct()
    df_arrival_filtered = df_curated_arrival.where(col("arrivalAirport_iatacode") == arr).dropna().distinct()
    
    # Join on flight_iatanumber and select only the departure columns
    df4 = df_departure_filtered.join(df_arrival_filtered, on="flight_iatanumber", how="inner").select(df_departure_filtered["*"]).distinct()
    
    # Union the results into df_reduced_departure
    if df_reduced_departure is None:
        df_reduced_departure = df4
    else:
        df_reduced_departure = df_reduced_departure.union(df4)

# Show the result if there is data
if df_reduced_departure:
    df_reduced_departure.show(truncate=False)
display(df_reduced_departure.count())

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
table_name = "departureFlight_Fact"

df_reduced_departure.write.mode("append").jdbc(url:jdbcUrl,table=table_name,properties=connectionProperties)


# COMMAND ----------

# MAGIC %md
# MAGIC copying departure fact table

# COMMAND ----------

from pyspark.sql.functions import col

# Load the DataFrames from the specified paths
df_curated_departure = spark.read.format("parquet").load("abfss://curatedlayer@aerooptimizesg.dfs.core.windows.net/curatedlayer/departureflight-Fact")
df_curated_arrival = spark.read.format("parquet").load("abfss://curatedlayer@aerooptimizesg.dfs.core.windows.net/curatedlayer/arrivalflight_Fact")

# Dictionary of departure and arrival airport codes
values = {
    "lax": "sfo",
    "jfk": "lax",
    "ord": "jfk",
    "ord": "lax",
    "atl": "mco",
    "las": "fll",
    "dfw": "mia"
}

df_reduced_departure = None

# Iterate over each dep and arr pair in the dictionary
for dep, arr in values.items():
    # Filter departure and arrival DataFrames based on dep and arr values
    df_departure_filtered = df_curated_departure.where(col("departureAirport_iatacode") == dep).dropna().distinct()
    df_arrival_filtered = df_curated_arrival.where(col("arrivalAirport_iatacode") == arr).dropna().distinct()
    
    # Join on flight_iatanumber and select only the departure columns
    df4 = df_departure_filtered.join(df_arrival_filtered, on="flight_iatanumber", how="inner").select(df_departure_filtered["*"]).distinct()
    
    # Union the results into df_reduced_departure
    if df_reduced_departure is None:
        df_reduced_departure = df4
    else:
        df_reduced_departure = df_reduced_departure.union(df4)

# Check if df_reduced_departure has any rows before displaying
if df_reduced_departure is not None and df_reduced_departure.count() > 0:
    df_reduced_departure.show(truncate=False)
    display(df_reduced_departure.count())

# JDBC connection properties
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
table_name = "departureFlight_Fact"

# Count the records in the original DataFrame before writing
original_count = df_reduced_departure.count()

# Write to SQL Server table
df_reduced_departure.write.mode("append").jdbc(url=jdbcUrl, table=table_name, properties=connectionProperties)

# Read the data back from SQL Server to verify
df_copied_data = spark.read.jdbc(
    url=jdbcUrl,
    table=table_name,
    properties=connectionProperties
)

# Count the records in the SQL Server table
copied_count = df_copied_data.count()

# Print the counts to verify
print(f"Original count: {original_count}")
print(f"Copied count: {copied_count}")

# Verify if data was copied successfully
if original_count == copied_count:
    print("Data copied successfully!")
else:
    print("Mismatch in record count; please verify the data.")


# COMMAND ----------

df1=spark.read.format("parquet").load("abfss://curatedlayer@aerooptimizesg.dfs.core.windows.net/curatedlayer/arrivalflight_Fact")
display(df1.printSchema())

# COMMAND ----------

# MAGIC %md
# MAGIC ArrivalFlightFact

# COMMAND ----------

from pyspark.sql.functions import col

# Load the DataFrames from the specified paths
df_curated_departure = spark.read.format("parquet").load("abfss://curatedlayer@aerooptimizesg.dfs.core.windows.net/curatedlayer/departureflight-Fact")
df_curated_arrival = spark.read.format("parquet").load("abfss://curatedlayer@aerooptimizesg.dfs.core.windows.net/curatedlayer/arrivalflight_Fact")

# Dictionary of departure and arrival airport codes
values = {
    "lax": "sfo",
    "jfk": "lax",
    "ord": "jfk",
    "ord": "lax",
    "atl": "mco",
    "las": "fll",
    "dfw": "mia"
}

df_reduced_arrival = None

# Iterate over each dep and arr pair in the dictionary
for dep, arr in values.items():
    # Filter departure and arrival DataFrames based on dep and arr values
    df_departure_filtered = df_curated_departure.where(col("departureAirport_iatacode") == dep).dropna().distinct()
    df_arrival_filtered = df_curated_arrival.where(col("arrivalAirport_iatacode") == arr).dropna().distinct()
    
    # Join on flight_iatanumber and select only the departure columns
    df4 = df_arrival_filtered.join(df_departure_filtered, on="flight_iatanumber", how="inner").select(df_arrival_filtered["*"]).distinct()
    
    # Union the results into df_reduced_departure
    if df_reduced_arrival is None:
        df_reduced_arrival= df4
    else:
        df_reduced_arrival = df_reduced_arrival.union(df4)

# Check if df_reduced_departure has any rows before displaying
if df_reduced_arrival is not None and df_reduced_arrival.count() > 0:
    df_reduced_arrival.show(truncate=False)
    display(df_reduced_arrival.count())

# JDBC connection properties
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
table_name = "ArrivalFlight"

# Count the records in the original DataFrame before writing
original_count = df_reduced_arrival.count()

# Write to SQL Server table
df_reduced_arrival.write.mode("append").jdbc(url=jdbcUrl, table=table_name, properties=connectionProperties)

# Read the data back from SQL Server to verify
df_copied_data = spark.read.jdbc(
    url=jdbcUrl,
    table=table_name,
    properties=connectionProperties
)

# Count the records in the SQL Server table
copied_count = df_copied_data.count()

# Print the counts to verify
print(f"Original count: {original_count}")
print(f"Copied count: {copied_count}")

# Verify if data was copied successfully
if original_count == copied_count:
    print("Data copied successfully!")
else:
    print("Mismatch in record count; please verify the data.")

# COMMAND ----------

# MAGIC %md
# MAGIC