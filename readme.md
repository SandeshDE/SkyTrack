# SkyTrack – Aviation Data Pipeline and Analytics

## 🔍 Overview

SkyTrack is an end-to-end aviation data analytics solution built to extract, process, and analyze flight operational data to understand how environmental factors influence flight delays. It uses a cloud-native data engineering pipeline to ingest real-time and historical flight data, weather data, and operational logs. The insights are delivered through visual dashboards powered by Power BI.

---

## 🌐 Technologies Used

* **Azure Data Factory (ADF)**: For orchestrating ETL workflows from Aviation Edge API
* **Azure Data Lake Storage Gen2 (ADLS)**: For scalable and secure storage of raw and processed data
* **Azure Databricks**: For transformation using PySpark and Delta Lake
* **Azure SQL Database**: For curated data access and ML model readiness
* **Power BI**: For real-time visualization

---

## ⚖️ Data Sources

* **Flight Operations API**: Arrival and departure events (e.g., LAX, SFO, ORD, JFK, MIA)
* **Weather APIs**: Environmental conditions affecting aviation delays
* **Time Range**: October 2023 to October 2024
* **Flight and Environmental Metadata**: Carrier codes, IATA routes, timestamps, weather types, delay categories

---

## 🚚 Ingestion Process (ADF)

* Dates are validated using a PySpark module inside Databricks
* A CSV input in Lookup activity provides start/end dates
* API keys are securely accessed from Azure Key Vault
* Copy activity in ADF calls Aviation Edge API and writes raw JSON to ADLS
* Ingested routes include: JFK-LAX, LAX-SFO, DFW-MIA, ORD-LAX
* Total Ingested Records: \~5.8 million rows

---

## ⚖️ Transformation Pipeline

### Raw to Conformance Layer:

* Data from ADLS is read and schema validated
* PySpark logic filters, cleanses, and normalizes JSON fields
* Parsed into structured Delta tables in the Conformance container

### Conformance to Curated Layer:

* Star Schema modeling:

  * **Fact Tables**: ArrivalFlight, DepartureFlight
  * **Dimensions**: Airline, Flight, Weather, Timestamp
* Transformed Delta tables are written to Curated container

### Optimization:

* Curated data exported to Parquet format
* Stored in Gold layer for cost-effective querying
* Data copied to Azure SQL Database using dynamic connection strings

---

## 🪡 Analytical Use Cases

* **Top 10 Delays by Weather Impact**:

  * Departure delays (e.g., JFK-LAX, ORD-LAX)
  * Arrival delays (e.g., LAX-SFO)
* **Flight-Weather Correlation**:

  * Delay types mapped to environmental tags (fog, wind, storms)
* **Performance Aggregation**:

  * Delay duration stats per airport pair

---

## 🔮 Storage Architecture (Medallion)

* **Bronze Layer**: Raw API data in JSON
* **Silver Layer**: Conformed, validated Delta tables
* **Gold Layer**: Optimized, enriched, analytics-ready Parquet format

---

## 🏛️ Visualization with Power BI

* Power BI dashboards show:

  * Route-specific delay distributions
  * Environmental factor breakdown
  * KPI tiles for delay count, avg delay time, % weather-related

---

## 🌐 Deployment Architecture

* ADF orchestrates ETL
* Databricks notebooks process and transform data
* Delta → Parquet → Azure SQL
* Power BI connects to Gold layer and SQL DB

---

## ⚒️ Future Enhancements

* Integrate live weather stream APIs
* Expand route coverage across EU/Asia
* Build predictive models for delay likelihood
* Enable alerts for weather-triggered operational risks

---



---

## 📄 License

This project is built for academic and professional portfolio purposes only.

---


* Tools Delivered: ETL pipelines, curated datasets, Power BI dashboards
<img width="1317" height="591" alt="image" src="https://github.com/user-attachments/assets/739a58c1-1a2a-4c43-bcf7-0ca4a22db814" />
<img width="1289" height="566" alt="image" src="https://github.com/user-attachments/assets/6c8e2ed3-4e30-4fae-ab10-ab6073b42045" />
<img width="1294" height="646" alt="image" src="https://github.com/user-attachments/assets/fb1e3abb-0cf1-453e-ae85-743ace3a8531" />
<img width="1359" height="648" alt="image" src="https://github.com/user-attachments/assets/7a7f68f1-aaed-41a6-ae0c-d82b3b65b983" />
<img width="1281" height="660" alt="image" src="https://github.com/user-attachments/assets/4fe37a41-2fe1-47ba-87a3-cbe165dc2f0f" />




