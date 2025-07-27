# Ingestion Phase – SkyTrack Aviation Data Pipeline

The Ingestion Phase of the SkyTrack Aviation Data Pipeline focuses on securely extracting flight and environmental data from external APIs, validating inputs, and storing raw results in Azure Data Lake Storage (ADLS) for further transformation.

---

## 📅 Time Frame

* Data collection spans from **October 11, 2023 to October 31, 2024**.
* Ingestion includes both **arrival** and **departure** flights.

---

## 🌐 APIs & Data Sources

* **Flight Data**: Aviation Edge API
* **Environmental/Weather Data**: External weather APIs (referenced in later phases)
* **Airports Covered**:

  * Arrivals: LAX, SFO, MIA
  * Departures: ORD, JFK, LAX

---

## 💡 Pipeline Overview (Azure Data Factory)

### 1. **Date Validation Logic**

* A custom Python module (executed in Azure Databricks) ensures date ranges sent to the Aviation Edge API comply with its allowed limits.
* Dates are dynamically fetched via **Lookup Activity** from a CSV file containing `start_date` and `end_date`.

### 2. **Secure API Key Management**

* API keys are stored securely in **Azure Key Vault**.
* Keys are retrieved dynamically and passed to the **Copy Activity** to avoid hardcoding.

### 3. **Copy Activity Configuration**

* **Source**: REST API (GET method)
* **Destination**: ADLS Gen2 (Raw container)
* **Format**: JSON
* **Partitioning**: Files stored by `origin`, `destination`, and date.
* **Fault Tolerance**: Retry mechanism and timeout settings are configured to ensure resilience.

### 4. **Dynamic Ingestion Control**

* The ingestion pipeline loops through airport codes and date ranges using **ForEach** and **SetVariable** activities.
* For each loop iteration:

  * API URL is dynamically constructed
  * Headers and parameters are injected
  * Results are stored into raw zone

---

## 🔢 Key Features

* ⛓️ **End-to-end automation** of flight data retrieval
* 🔐 **Secure** API key handling (no hardcoding)
* 🔁 **Dynamic looping** over multiple airport/date combinations
* 📂 **Partitioned storage** in ADLS Gen2 for efficient downstream processing
* 📈 **Scalability** to support large volumes (\~5.8M rows ingested)

---

## 👁‍🗨️ Sample Output

* Each API response is stored in its own JSON file.
* Example path:

  ```
  abfss://raw@aviationdatalake.dfs.core.windows.net/flight-data/arrivals/LAX/2023-10-11.json
  ```
* Metadata logged for monitoring: request timestamp, airport, status, rows ingested.

---

## ✅ Outcome

The Ingestion Phase establishes a robust, secure, and dynamic process to pull aviation data into the system, ensuring quality and availability for downstream transformation and analysis.

<img width="1168" height="610" alt="image" src="https://github.com/user-attachments/assets/c2c0b6f1-0e76-4fba-abba-515cad45d1ce" />


