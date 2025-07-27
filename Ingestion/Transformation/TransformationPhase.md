# Transformation Phase – SkyTrack Aviation Data Pipeline

The Transformation Phase of the SkyTrack project focuses on converting raw flight data into structured, validated, and enriched datasets using a layered approach following the Medallion architecture: **Raw → Conformance → Curated**.

---

## 📑 Overview

* **Source**: Raw JSON data stored in ADLS Gen2 (from Ingestion phase)
* **Tool**: Azure Databricks (PySpark Notebooks)
* **Output**: Structured Delta and Parquet datasets in Conformance and Curated zones
* **Volume**: Over 5.8 million rows processed across multiple airports

---

## 🤖 Step-by-Step Breakdown

### 1. **Read & Schema Validation (Raw to Conformance)**

* JSON files are loaded from the Raw container into PySpark DataFrames.
* Schema is explicitly defined to ensure consistent structure.
* Rows failing schema checks are logged and dropped.

### 2. **Date and Airport-Based Partitioning**

* Files are organized based on origin, destination, and timestamp.
* Each record is tagged with metadata (airport, request time, etc.).

### 3. **Transform Fields**

* **Flattening** nested structures (e.g., `data`, `arrival`, `departure`, `airline` objects)
* Extract key columns:

  * Flight IATA/ICAO codes
  * Actual/Scheduled departure & arrival timestamps
  * Gate, terminal, runway data
  * Delay minutes and reason codes

### 4. **Cleaning and Deduplication**

* Remove duplicates using a composite key: flight number + departure date + airport
* Null value handling for critical columns (e.g., actual times, delay reasons)

### 5. **Delta Table Write (Conformance Layer)**

* Save the cleaned and structured data in Delta format using `overwrite` mode:

  ```python
  df.write.format("delta").mode("overwrite").save("<conformance-path>")
  ```
* Partitioned by `airport` and `year-month` for performance

### 6. **Join with Weather & Enrichment (Conformance to Curated)**

* Join with environmental datasets using keys: `airport + date + hour`
* Calculate delay impact features:

  * Was delay weather-related?
  * Type of environmental condition at time of delay
* Add delay category tags (minor, moderate, severe)

### 7. **Fact and Dimension Table Modeling**

* **Fact Table**: FlightEvents (arrivals and departures)
* **Dimension Tables**:

  * Airline
  * Flight
  * Airport
  * Weather
  * Time (calendar breakdown)

### 8. **Write to Parquet & SQL DB (Curated Layer)**

* Curated tables are written in Parquet format
* Loaded into Azure SQL Database using connection string
* Filtered by route: JFK-LAX, LAX-SFO, ORD-LAX, DFW-MIA

---

## ⚖️ Medallion Architecture Flow

```
Bronze (Raw JSON) → Silver (Validated Delta Tables) → Gold (Curated Parquet + SQL DB)
```

* Data gradually becomes cleaner, more enriched, and more analytics-ready at each stage

---

## 🔢 Key Features

* ✅ Schema enforcement and consistency
* ✅ Integrated weather and delay data
* ✅ Star Schema design for fast query performance
* ✅ Write once, read many model (Delta + Parquet)

---

## ✅ Outcome

The Transformation Phase ensures data quality, consistency, and relevance, transforming large-scale JSON data into a unified analytical model ready for Power BI dashboards and predictive modeling.

<img width="1101" height="568" alt="image" src="https://github.com/user-attachments/assets/47bd6bf0-bd88-482c-a4b4-1048c25b2ac6" />

