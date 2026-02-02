# Airflow Pipeline
This folder contains the Apache Airflow DAG used to orchestrate the retail sales analytics pipeline.

## DAG: retail_sales_pipeline
Tasks:
1. etl_cleaning – Runs Spark ETL job to clean raw data and store it in Hive.
2. feature_engineering – Creates the required features from cleaned data.
3. forecasting – Trains ML model and generates sales forecast.

## Technologies Used
- Apache Airflow
- Apache Spark
- HDFS
- Hive

## Execution Order
ETL → Feature Engineering → Forecasting
