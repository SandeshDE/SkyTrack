# Databricks notebook source
service_credential = dbutils.secrets.get(scope="Key-Vault-DataBricksSPSecret",key="databricksSpSecret")

spark.conf.set("fs.azure.account.auth.type.aerooptimizesg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.aerooptimizesg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.aerooptimizesg.dfs.core.windows.net", "1c0ed18f-fef9-4706-bb75-eba21d0542c4")
spark.conf.set("fs.azure.account.oauth2.client.secret.aerooptimizesg.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.aerooptimizesg.dfs.core.windows.net", "https://login.microsoftonline.com/9b2c18e1-54fb-4641-a931-e1b8581e8e81/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@aerooptimizesg.dfs.core.windows.net/"))

# COMMAND ----------

import pandas as pd

# COMMAND ----------

df=spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://raw@aerooptimizesg.dfs.core.windows.net/Dates.csv")
df.show()

# COMMAND ----------

"""
dates_row=df.first() #creating a single row object
# Print each value in the row
print(f"Start Date: {dates_row['start_date']}")
print(f"End Date: {dates_row['end_date']}")
"""

# COMMAND ----------

"""
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json

# Load the CSV file
csv_path = "abfss://raw@aerooptimizesg.dfs.core.windows.net/Dates.csv"  # Replace with your actual path
df = spark.read.option("header", "true").csv(csv_path)

# Extract start_date and end_date from the first row of the DataFrame
dates_row = df.first()
start_date = dates_row['start_date']
end_date = dates_row['end_date']

# Function to check date validity
def chk_dates(start_date, end_date):
    x = datetime.now()
    start_date_Dt = datetime.strptime(start_date, "%m/%d/%Y")
    end_date_Dt = datetime.strptime(end_date, "%m/%d/%Y")
    result = {}

    if start_date_Dt >= (x - relativedelta(years=1)):
        result['valid_start_date'] = start_date_Dt.strftime("%Y-%m-%d")
    else:
        result['error'] = "Start date is more than 1 year ago."
        return json.dumps(result)

    if end_date_Dt >= (x - relativedelta(years=1)):
        result['valid_end_date'] = end_date_Dt.strftime("%Y-%m-%d")
    else:
        result['error'] = "End date is more than 1 year ago."

    return json.dumps(result)

# Call the function with CSV dates and pass the result back to ADF
result = chk_dates(start_date, end_date)
#print(result)
dbutils.notebook.exit(result)

"""


# COMMAND ----------


"""
# Define the widgets to capture input parameters
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")

# Now you can retrieve the widget values
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

# Your existing function to check dates
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json

def chk_dates(start_date, end_date):
    # Current date
    x = datetime.now()

    # Convert string to datetime
    start_date_Dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_Dt = datetime.strptime(end_date, "%Y-%m-%d")

    # Initialize result dict
    result = {}

    # Check if start date is within the last year
    if start_date_Dt >= (x - relativedelta(years=1)):
        result['valid_start_date'] = start_date
    else:
        result['error'] = "Start date is more than 1 year ago."
        return json.dumps(result)

    # Check if end date is not older than 1 year
    one_year_ago = x - relativedelta(years=1)
    if end_date_Dt >= one_year_ago:
        result['valid_end_date'] = end_date
    else:
        result['error'] = "End date is more than 1 year ago."

    # Return the result as JSON
    return json.dumps(result)

# Call the function with the widget values
result = chk_dates(start_date, end_date)

# Exit the notebook and pass the result back to ADF
dbutils.notebook.exit(result)

"""


# COMMAND ----------

# Define the widgets to capture input parameters
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")

# Retrieve the widget values
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

# Import necessary modules
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json

# Function to parse dates with multiple formats
def parse_date(date_str):
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%Y/%m/%d"):     #yyyy-mm-dd
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Date '{date_str}' does not match any recognized format.")

# Function to check dates
def chk_dates(start_date, end_date):
    # Current date
    x = datetime.now()

    # Convert string to datetime
    try:
        start_date_Dt = parse_date(start_date)
        end_date_Dt = parse_date(end_date)
    except ValueError as e:
        return json.dumps({"error": str(e)})

    # Initialize result dict
    result = {}

    # Check if start date is within the last year
    if start_date_Dt >= (x - relativedelta(years=1)):
        result['valid_start_date'] = start_date_Dt.strftime("%Y-%m-%d")
    else:
        result['error'] = "Start date is more than 1 year ago."
        return json.dumps(result)

    # Check if end date is not older than 1 year
    one_year_ago = x - relativedelta(years=1)
    if end_date_Dt >= one_year_ago:
        result['valid_end_date'] = end_date_Dt.strftime("%Y-%m-%d")
    else:
        result['error'] = "End date is more than 1 year ago."

    # Return the result as JSON
    return json.dumps(result)

# Call the function with the widget values
result = chk_dates(start_date, end_date)

# Exit the notebook and pass the result back to ADF
dbutils.notebook.exit(result)
