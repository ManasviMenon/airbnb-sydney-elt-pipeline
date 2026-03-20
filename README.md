# Airbnb Sydney ELT Pipeline

Built a full data pipeline to process 12 months of Sydney Airbnb listings alongside 
ABS Census data and it turns raw CSVs into a clean, queryable warehouse with real 
business insights at the end.

## Tech Stack
Apache Airflow · dbt Cloud · PostgreSQL · Google Cloud Platform

## How It Works
The pipeline follows a Medallion Architecture, moving data through three stages:
- **Bronze** — Raw data lands here, ingested monthly via Airflow from GCS
- **Silver** — dbt cleans, deduplicates, and standardizes everything
- **Gold** — Analytics-ready star schema with dimension tables, fact tables, 
  and datamarts. SCD Type 2 snapshots preserve historical changes to hosts 
  and properties over time.

## What I Built
- Automated monthly ingestion DAG running on GCP Cloud Composer
- SCD Type 2 tracking so the pipeline remembers how hosts and properties 
  looked at any point in time — not just their latest state
- Joined Airbnb listings with 2016 ABS Census data at the LGA level to add 
  demographic and economic context
- 3 analytical datamarts covering listing neighbourhoods, property types, 
  and host neighbourhoods

## What I Found
- Higher-income LGAs with smaller households consistently outperform on revenue
- Median age and revenue per listing have a **0.62 correlation** : older 
  Neighbourhoods tend to earn more
- Entire apartments for 2–4 guests are the sweet spot in top neighbourhoods
- 94.3% of hosts with multiple listings keep them all within the same LGA

## Files
| File | Description |
|------|-------------|
| `models/` | dbt Bronze, Silver & Gold models |
| `snapshots/` | SCD Type 2 snapshot definitions |
| `airflow_dag.py` | Combined Airflow ingestion DAG |
| `part_1.sql` / `part_4.sql` | SQL queries |
| `report.pdf` | Full project report |
