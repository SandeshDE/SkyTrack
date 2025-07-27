# Weather Data Integration Phase – SkyTrack Aviation Data Pipeline

This phase integrates external weather data into the aviation pipeline to enrich flight delay analysis and correlate delays with specific environmental conditions. It is critical for identifying cause-and-effect relationships between adverse weather events and disruptions in flight schedules.

---

## 🌌 Purpose

To align **historical weather conditions** with **arrival/departure records** and assess the extent of delays caused by specific weather types (fog, wind, rain, storms).

---

## 📖 Data Sources

* **External Weather APIs** (e.g., OpenWeatherMap, NOAA, AviationWeather.gov)
* **Date Range**: October 2023 to October 2024
* **Airports Mapped**: JFK, LAX, SFO, MIA, ORD, DFW
* **Granularity**: Hourly weather data per airport location

---

## 🔹 Extraction Process (ETL)

### 1. **Weather API Ingestion**

* REST-based API calls
* Authenticated via token or key
* Data includes:

  * Temperature, wind speed, humidity
  * Precipitation type and intensity
  * Visibility levels
  * Weather condition tags (fog, storm, drizzle, clear, etc.)

### 2. **Data Normalization**

* Parsed into structured JSON schema
* Converted to Delta tables
* Time and location (airport code) normalized to match flight event granularity

### 3. **Storage**

* Raw weather JSON written to Bronze Layer in ADLS Gen2
* Cleaned and validated tables promoted to Silver Layer
* Partitioned by airport and date

---

## ⚖️ Data Join & Enrichment (in Databricks)

### Matching Logic:

* Join `FlightEvents` table with `WeatherEvents` using:

  * `airport_code`
  * `date` and `hour`

### Feature Engineering:

* `is_weather_delay`: Boolean flag if flight delayed during adverse condition
* `weather_condition`: Derived label from API (e.g., fog, storm)
* `delay_category`: Mapped using aviation standard definitions (minor, moderate, severe)

### Output:

* Weather-enriched Flight Fact Table
* Available as Delta and Parquet

---

## 🤖 Sample Feature Columns

| Column Name        | Type    | Description                     |
| ------------------ | ------- | ------------------------------- |
| airport\_code      | string  | IATA code of airport            |
| timestamp\_hour    | string  | Rounded timestamp by hour       |
| weather\_condition | string  | Fog, Storm, Clear, etc.         |
| is\_weather\_delay | boolean | True if weather impacted flight |
| delay\_minutes     | int     | Delay duration in minutes       |
| delay\_category    | string  | Minor, Moderate, Severe         |

---

## 🚀 Impact on Analysis

* Enables **top 10 delay reports** filtered by weather
* Powers visualizations for weather-related disruptions
* Improves accuracy of **root-cause analysis** in Power BI
* Feeds features into future predictive models for delay forecasting

---

## 📄 Outcome

The Weather Data Integration Phase bridges environmental data with flight activity logs, enabling more accurate and explainable analysis of delay patterns. This integration is essential for real-time insights and historical reporting.

<img width="820" height="477" alt="image" src="https://github.com/user-attachments/assets/0ab93dad-9596-4952-a964-277f4b1b2c77" />

