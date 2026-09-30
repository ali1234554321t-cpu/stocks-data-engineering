# 📈 Real-Time Stocks Data Engineering Platform

An end-to-end data engineering platform for collecting, streaming, transforming, storing, orchestrating, and analyzing stock market data.

The project combines **real-time stock quotes** with **historical market data** and delivers a unified analytical dataset ready for Business Intelligence and reporting.

---

## 🏗️ Project Overview

This project simulates a production-style stock market data platform built around two main data flows:

### Real-Time Data Pipeline

Real-time stock quotes are collected from the Finnhub API, published to Apache Kafka, processed using Spark Structured Streaming, and loaded into Snowflake.

### Historical Data Pipeline

Historical OHLCV market data is collected using Yahoo Finance and loaded into Snowflake for long-term analysis.

Both datasets are transformed using dbt into a unified analytical model called:

`FCT_STOCK_MARKET`

---

## 🔄 End-to-End Architecture

```text
                         ┌─────────────────┐
                         │   Finnhub API   │
                         └────────┬────────┘
                                  │
                                  ▼
                         ┌─────────────────┐
                         │ Python Ingestion│
                         └────────┬────────┘
                                  │
                                  ▼
                         ┌─────────────────┐
                         │  Kafka Producer │
                         └────────┬────────┘
                                  │
                                  ▼
                         ┌─────────────────┐
                         │  Apache Kafka   │
                         └────────┬────────┘
                                  │
                                  ▼
                    ┌──────────────────────────┐
                    │ Spark Structured         │
                    │ Streaming                │
                    └────────────┬─────────────┘
                                 │
                                 ▼
                         ┌─────────────────┐
                         │ Snowflake RAW   │
                         │ STOCK_PRICES    │
                         └────────┬────────┘
                                  │
                                  │
Yahoo Finance                    │
     │                           │
     ▼                           │
┌────────────────────┐           │
│ Historical Backfill│           │
└─────────┬──────────┘           │
          │                      │
          ▼                      │
┌──────────────────────────────┐ │
│ Snowflake RAW                │ │
│ STOCK_PRICES_HISTORY         │ │
└──────────────┬───────────────┘ │
               │                 │
               └────────┬────────┘
                        ▼
               ┌─────────────────┐
               │   dbt Staging   │
               └────────┬────────┘
                        ▼
               ┌─────────────────┐
               │ FCT_STOCK_MARKET│
               └────────┬────────┘
                        ▲
                        │
               ┌─────────────────┐
               │ Airflow          │
               │ Orchestration    │
               │ & Scheduling     │
               └─────────────────┘
                        │
                        ▼
               ┌─────────────────┐
               │    Power BI     │
               └─────────────────┘
```

> The architecture diagram image will be added later.

---

## 🛠️ Technologies

| Technology     | Purpose                                 |
| -------------- | --------------------------------------- |
| Python         | Data ingestion and historical backfill  |
| Finnhub API    | Real-time stock quote source            |
| Yahoo Finance  | Historical stock market data            |
| Apache Kafka   | Event streaming and message transport   |
| Apache Spark   | Stream processing                       |
| Snowflake      | Cloud data warehouse                    |
| dbt            | Data transformation and data quality    |
| Apache Airflow | Pipeline orchestration and scheduling   |
| Docker         | Containerized services                  |
| Power BI       | Business intelligence and visualization |

---

## 📊 Data Model

The final analytical dataset is:

`FCT_STOCK_MARKET`

It combines historical and real-time stock information into one model.

### Main Columns

| Column             | Description                           |
| ------------------ | ------------------------------------- |
| `SYMBOL`           | Stock ticker symbol                   |
| `EVENT_TIME`       | Market-related event timestamp        |
| `PRICE`            | Stock price                           |
| `CHANGE`           | Price change from the previous value  |
| `CHANGE_PERCENT`   | Percentage price change               |
| `HIGH`             | Highest recorded price                |
| `LOW`              | Lowest recorded price                 |
| `OPEN`             | Opening price                         |
| `PREVIOUS_CLOSE`   | Previous closing price                |
| `TRADING_RANGE`    | `HIGH - LOW`                          |
| `PRICE_CHANGE_ABS` | Absolute price change                 |
| `PRICE_DIRECTION`  | `UP`, `DOWN`, or `UNCHANGED`          |
| `VOLUME`           | Trading volume for historical records |
| `DIVIDENDS`        | Dividend value when available         |
| `STOCK_SPLITS`     | Stock split value when available      |
| `DATA_SOURCE`      | `HISTORICAL` or `REALTIME`            |

---

## 🧱 dbt Layer

The dbt project contains staging models and a final mart.

```text
stocks_dbt/
│
├── models/
│   ├── staging/
│   │   ├── sources.yml
│   │   ├── stg_stock_prices.sql
│   │   └── stg_stock_prices_history.sql
│   │
│   └── marts/
│       ├── fct_stock_market.sql
│       └── schema.yml
│
├── dbt_project.yml
├── packages.yml
└── package-lock.yml
```

### Staging Models

`stg_stock_prices`

Cleans and deduplicates real-time stock data.

`stg_stock_prices_history`

Transforms historical OHLCV data and calculates previous close, price change, and percentage change.

### Mart

`fct_stock_market`

Combines the historical and real-time datasets into a unified analytics layer.

---

## ✅ Data Quality

The dbt project includes tests for:

* Non-null stock symbols
* Non-null event timestamps
* Non-null prices
* Non-null price directions
* Unique `SYMBOL + EVENT_TIME` combinations

Latest dbt validation:

```text
8/8 tests and models passed
0 errors
0 warnings
```

---

## ⏱️ Airflow Orchestration

Apache Airflow is used to orchestrate the stock data workflow.

The pipeline is scheduled to run periodically, supporting recurring ingestion and downstream dbt execution.

Airflow is responsible for:

* Pipeline scheduling
* Task orchestration
* Dependency management
* Triggering downstream transformations

---

## 🌊 Streaming Architecture

The real-time path follows an event-driven architecture:

```text
Finnhub
   ↓
Python
   ↓
Kafka Producer
   ↓
Kafka Topic
   ↓
Spark Structured Streaming
   ↓
Snowflake
```

This separates data ingestion from stream processing and storage.

---

## 🕒 Historical Backfill

Historical market data is loaded separately using Yahoo Finance.

Current backfill:

```text
Stocks: 15
Historical rows: 3,765
Period: ~1 year
```

This provides enough historical data for trend analysis and BI reporting.

---

## 📦 Current Dataset

The final `FCT_STOCK_MARKET` model currently contains:

```text
Historical records : 3,765
Real-time records  : 20
Total records      : 3,785
```

The platform tracks 15 stock symbols.

---

## 📁 Project Structure

```text
stocks-data-engineering/
│
├── airflow/
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   ├── config/
│   └── dags/
│
├── ingestion/
│   ├── historical_backfill.py
│   ├── kafka_producer.py
│   ├── kafka_producer_batch.py
│   ├── kafka_consumer.py
│   ├── save_raw_data.py
│   ├── stock_api.py
│   └── test_stock_api.py
│
├── streaming/
│   ├── spark_kafka_stream.py
│   └── spark_kafka_to_snowflake.py
│
├── stocks_dbt/
│   ├── models/
│   ├── macros/
│   ├── tests/
│   ├── dbt_project.yml
│   ├── packages.yml
│   └── package-lock.yml
│
├── config.py
├── docker-compose.yml
├── test_api.py
├── test_env.py
├── test_snowflake.py
├── test_spark_snowflake.py
└── .gitignore
```

---

## 🔐 Security

Sensitive credentials are stored outside the source code using environment variables.

The repository intentionally excludes:

```text
.env
.venv/
logs/
checkpoints/
data/raw/
stocks_dbt/target/
stocks_dbt/dbt_packages/
```

No API keys or Snowflake passwords are stored directly in the project source code.

---

## 🚀 Running the Project

### 1. Clone the repository

```bash
git clone https://github.com/ali1234554321t-cpu/stocks-data-engineering.git
cd stocks-data-engineering
```

### 2. Configure environment variables

Create your local `.env` file with the required Snowflake and Finnhub credentials.

Example:

```env
FINNHUB_API_KEY=your_finnhub_key

SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_WAREHOUSE=your_warehouse
```

> Never commit your `.env` file.

### 3. Start the required services

Use Docker Compose for the project services.

```bash
docker compose up -d
```

### 4. Run historical backfill

```bash
python ingestion/historical_backfill.py
```

### 5. Run the streaming pipeline

The streaming components publish and process stock events through Kafka and Spark Structured Streaming.

### 6. Run dbt

```bash
cd stocks_dbt
dbt build
```

### 7. Open Power BI

Connect Power BI to Snowflake and use:

`FCT_STOCK_MARKET`

as the main analytics dataset.

---

## 📈 Power BI

The project includes two main analytical pages.

### Market Overview

Provides a market-level overview with:

* Total stocks
* Average price
* Total trading volume
* Average change percentage
* Market health indicator
* Average price by stock
* Average daily change
* Trading range
* Historical price trend
* Interactive slicers

### Stock Deep Dive

Provides detailed analysis for a selected stock:

* Latest price
* Latest change percentage
* Highest price
* Lowest price
* Historical price trend
* Current change gauge
* Price direction distribution
* Price vs trading volume

Power BI screenshots can be added here later.

---

## 🎯 Project Goals

This project was designed to demonstrate practical experience with:

* Real-time data ingestion
* Event streaming
* Stream processing
* Cloud data warehousing
* Historical data engineering
* ELT transformation
* Data quality testing
* Workflow orchestration
* Business intelligence

---


---

## 👨‍💻 Author

**Ali Rabea Ahmed**

Data Analyst | Data Engineering Enthusiast

GitHub:
https://github.com/ali1234554321t-cpu
