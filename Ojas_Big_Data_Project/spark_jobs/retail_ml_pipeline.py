from airflow import DAG
from airflow.operators.bash import *
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="retail_sales_ml_pipeline",
    default_args=default_args,
    schedule_interval=None,   # manual trigger
    catchup=False,
    description="End-to-end Retail ML Pipeline using Hive + Spark",
) as dag:


    refresh_mv = BashOperator(
        task_id="refresh_materialized_view",
        bash_command="""
        beeline -u jdbc:hive2://localhost:10000/project \
        -e "REFRESH MATERIALIZED VIEW mv_train_daily_sales"
        """
    )


    create_train_features = BashOperator(
        task_id="create_train_features_table",
        bash_command="""
        beeline -u jdbc:hive2://localhost:10000/project \
        -e "
        DROP TABLE IF EXISTS train_features;
        CREATE TABLE train_features
        STORED AS PARQUET
        AS
        SELECT
            `date`,
            store_nbr,
            family,
            total_sales,
            total_transactions,
            avg_oil_price
        FROM mv_train_daily_sales;
        "
        """
    )


    train_model = BashOperator(
        task_id="train_sales_forecast_model",
        bash_command="""
        spark-submit \
        /home/sunbeam/Desktop/DBDA_AUG_2025/Unofficial/Big_Data_Project/spark_jobs/train_forecast_model.py
        """
    )


    forecast_sales = BashOperator(
        task_id="forecast_sales",
        bash_command="""
        spark-submit \
        /home/sunbeam/Desktop/DBDA_AUG_2025/Unofficial/Big_Data_Project/spark_jobs/forecast_sales.py
        """
    )


    refresh_mv >> create_train_features >> train_model >> forecast_sales
