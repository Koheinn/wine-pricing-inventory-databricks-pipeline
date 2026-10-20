# Wine Pricing & Inventory — Databricks Data Pipeline

An end-to-end data engineering project that processes wine catalog data and supplier price updates using **Databricks, PySpark, and Lakeflow Spark Declarative Pipelines**.

The pipeline follows the **Medallion Architecture (Bronze → Silver → Gold)** to ingest raw data, validate and transform records, and maintain current wine prices alongside historical price changes.

## Project Overview

Wine pricing data can come from multiple sources and may change over time. This project demonstrates how to build a structured data pipeline that processes an initial wine catalog and continuously ingests supplier price updates.

The pipeline separates raw ingestion, data quality enforcement, and business-ready price tracking into three layers:

- **Bronze:** Ingests the initial wine catalog and streams incoming supplier price updates.
- **Silver:** Selects relevant fields, standardizes column names, converts dates, and enforces price validation rules.
- **Gold:** Maintains the latest known price for each wine and preserves historical price changes using Slowly Changing Dimensions (SCD).

## Architecture

```text
                    SOURCE DATA
                         |
             +-----------+-----------+
             |                       |
      Initial Wine CSV        Supplier Price Feeds
             |                       |
             v                       v
       +-----------+           +-----------+
       |  BRONZE   |           |  BRONZE   |
       | Wine Data |           |  Updates  |
       +-----------+           +-----------+
             |                       |
             v                       v
       +-----------+           +-----------+
       |  SILVER   |           |  SILVER   |
       | Wine      |           | Validated |
       | Catalog   |           | Updates   |
       +-----------+           +-----------+
                                     |
                      +--------------+--------------+
                      |                             |
                      v                             v
              +----------------+          +----------------+
              |      GOLD      |          |      GOLD      |
              | Current Prices |          | Price History  |
              |    SCD Type 1  |          |    SCD Type 2  |
              +----------------+          +----------------+
```

## Technology Stack

- **Databricks:** Managed data engineering and pipeline execution environment.
- **Python:** Pipeline implementation.
- **PySpark:** Distributed data processing and transformations.
- **Lakeflow Spark Declarative Pipelines:** Declarative pipeline definitions and data quality management.
- **Auto Loader:** Incremental ingestion of supplier CSV files using cloudFiles.
- **Delta tables:** Structured storage for downstream analytics, subject to the configured pipeline and catalog.
- **Medallion Architecture:** Separation of raw, cleaned, and business-ready data.
- **SCD Type 1 and Type 2:** Current-state and historical change tracking.

## Repository Structure

```text
wine-pricing-inventory-databricks-pipeline/
├── data/
│   └──                         # Project source data files
├── wine-pricing-inventory/
│   └── transformations/
│       ├── bronze.py           # Raw ingestion
│       ├── silver.py           # Data cleaning and validation
│       └── gold.py             # Current prices and price history
├── LICENSE
└── README.md
```

The exact contents of the `data/` directory depend on the source files committed to the repository.

## Pipeline Implementation

### 1. Bronze Layer — Raw Ingestion

**File:** `wine-pricing-inventory/transformations/bronze.py`

The Bronze layer creates two datasets.

#### `bronze_wines`

Reads the initial wine catalog from a CSV file stored in a Databricks Unity Catalog Volume.

Key operations:

- Reads CSV data with headers.
- Infers column types.
- Drops the `_c0` index column.
- Adds an `ingestion_time` timestamp to record when the data was ingested.

#### `bronze_price_updates`

Incrementally ingests supplier price updates using Apache Spark Auto Loader.

Key operations:

- Uses `spark.readStream` and the `cloudFiles` source.
- Reads CSV files from the supplier updates directory.
- Infers column types and recognizes headers.
- Adds an `ingestion_time` timestamp.

This design separates the initial catalog ingestion from incoming supplier price updates.

### 2. Silver Layer — Cleaning and Data Quality

**File:** `wine-pricing-inventory/transformations/silver.py`

The Silver layer prepares the data for downstream processing.

#### `silver_wines`

Reads `bronze_wines` and selects the following fields:

- `name`
- `region`
- `variety`
- `rating`
- `price`
- `notes`

A data quality expectation named `valid_price` drops records where `price` is not greater than zero.

#### `silver_price_updates`

Reads the streaming Bronze supplier updates and standardizes the schema.

Key transformations:

- Renames `wine_name` to `name`.
- Retains `price`.
- Converts `effective_date` to a date type.
- Enforces the `valid_price_update` expectation.

The `expect_or_fail` rule fails the relevant pipeline update if records violate the requirement that `price > 0`, rather than silently accepting invalid price updates.

### 3. Gold Layer — Business-Ready Price Tables

**File:** `wine-pricing-inventory/transformations/gold.py`

The Gold layer maintains two tables using `dlt.create_streaming_table` and `dlt.apply_changes`.

#### `gold_current_prices` — SCD Type 1

Maintains the latest available price record for each wine.

Configuration:

- **Target:** `gold_current_prices`
- **Source:** `silver_price_updates`
- **Key:** `name`
- **Sequence column:** `effective_date`
- **Change tracking:** SCD Type 1

SCD Type 1 updates the existing record with the latest applicable change, without preserving previous versions in the target table.

#### `gold_price_history` — SCD Type 2

Maintains a historical record of changes for each wine.

Configuration:

- **Target:** `gold_price_history`
- **Source:** `silver_price_updates`
- **Key:** `name`
- **Sequence column:** `effective_date`
- **Change tracking:** SCD Type 2

SCD Type 2 preserves historical versions of records so that price changes can be analyzed over time. The pipeline uses the framework's SCD Type 2 change-tracking behavior rather than implementing manual history-management logic.

## Data Quality Rules

| Dataset | Rule | Behavior |
|---|---|---|
| `silver_wines` | `price > 0` | Drops invalid records |
| `silver_price_updates` | `price > 0` | Fails the pipeline update when the expectation is violated |

These rules help prevent non-positive prices from entering the cleaned datasets and downstream price tables.

## Data Sources and Configuration

The current implementation expects the following Databricks Volume paths:

**Initial wine catalog**

```text
/Volumes/workspace/default/raw/inventory/wine-prices.csv
```

**Supplier price updates**

```text
/Volumes/workspace/default/raw/inventory/price_updates/
```

These paths are specific to the current Databricks workspace and Unity Catalog configuration. Update them if you deploy the project in another environment.

The supplier updates directory is intended to receive new CSV files that Auto Loader can discover and process.

Expected source fields include:

**Initial wine catalog**

`name`, `region`, `variety`, `rating`, `price`, `notes`

**Supplier price updates**

`wine_name`, `price`, `effective_date`

The initial catalog may also contain an `_c0` column, which the Bronze transformation drops.

## How to Run the Project

### Prerequisites

- Access to a Databricks workspace.
- Permissions to use the required catalog, schema, Volume, and pipeline resources.
- Source CSV files in the configured Volume paths.
- A pipeline environment that supports the Lakeflow Spark Declarative Pipelines APIs used in this project.

### Step 1: Prepare the source data

Upload the initial wine catalog to:

```text
/Volumes/workspace/default/raw/inventory/wine-prices.csv
```

Place supplier price update CSV files in:

```text
/Volumes/workspace/default/raw/inventory/price_updates/
```

### Step 2: Import the transformation files

Import the Python files from the `wine-pricing-inventory/transformations/` directory into the Databricks workspace or Git folder.

### Step 3: Configure the pipeline

Create a Lakeflow Spark Declarative Pipelines pipeline and configure it to include the transformation files.

Confirm that the pipeline uses the appropriate catalog, schema, permissions, and storage settings for your environment.

### Step 4: Run the pipeline

Start the pipeline and review its update status, event logs, and data quality results.

The Bronze and Silver datasets should be created before the Gold outputs can be populated successfully.

### Step 5: Verify the Gold outputs

Query the resulting tables using the catalog and schema configured for your pipeline.

For example, after replacing the placeholder names with your actual catalog and schema:

```sql
SELECT *
FROM your_catalog.your_schema.gold_current_prices;
```

```sql
SELECT *
FROM your_catalog.your_schema.gold_price_history;
```

The final table names depend on your configured pipeline target and publishing settings.

## Expected Outcomes

After a successful pipeline run, the project is designed to provide:

- An ingested wine catalog with ingestion timestamps.
- Incrementally processed supplier price updates.
- Cleaned and validated wine records.
- A current-price table for downstream consumption.
- A historical price table for analyzing changes over time.

Actual results depend on the input data, pipeline configuration, and successful processing of the source files.

## Potential Improvements

Future enhancements could include:

- Parameterizing catalog, schema, and Volume paths.
- Adding automated data quality tests.
- Handling duplicate supplier updates and conflicting effective dates.
- Adding explicit business keys or wine identifiers instead of relying solely on wine names.
- Implementing monitoring and alerting for failed pipeline updates.
- Adding automated deployment using Databricks Asset Bundles.
- Creating dashboards to visualize current prices and historical price trends.
- Adding a CI/CD workflow to validate and deploy pipeline changes.

## Learning Objectives

This project demonstrates practical data engineering concepts:

- Building a Medallion Architecture pipeline.
- Ingesting batch and incremental file data.
- Applying transformations with PySpark.
- Enforcing data quality expectations.
- Processing changes with SCD Type 1 and Type 2.
- Organizing pipeline code for version control and collaboration.

## Author

**Heinn Htet Zan — Koheinn**

- GitHub: https://github.com/Koheinn
- Portfolio: https://heinn-htet-zan.vercel.app/
- LinkedIn: https://www.linkedin.com/in/heinn-htet-zan-040794291/

## License

This project is distributed under the MIT License. See [LICENSE](LICENSE) for details.

---

*Built as a hands-on data engineering project using Databricks, PySpark, and Medallion Architecture.*
