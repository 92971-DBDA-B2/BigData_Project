from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "amaan",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    dag_id="retail_sales_pipeline",
    default_args=default_args,
    description="Retail Sales Analytics & Forecasting Pipeline",
    schedule="@daily",
    catchup=False
)

common_env = """
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/home/amaan/hadoop-3.3.2
export HIVE_HOME=/home/amaan/apache-hive-3.1.3-bin
export SPARK_HOME=/home/amaan/spark-3.5.7-bin-hadoop3
export PATH=$SPARK_HOME/bin:$PATH
"""

etl_task = BashOperator(
    task_id="etl_cleaning",
    bash_command=common_env + """
spark-submit /home/amaan/Desktop/PGDBDA/BigData_Project/ETL.py
""",
    dag=dag
)

feature_task = BashOperator(
    task_id="feature_engineering",
    bash_command=common_env + """
spark-submit /home/amaan/Desktop/PGDBDA/BigData_Project/Feature.py
""",
    dag=dag
)

forecast_task = BashOperator(
    task_id="forecasting",
    bash_command=common_env + """
spark-submit /home/amaan/Desktop/PGDBDA/BigData_Project/Forecast.py
""",
    dag=dag
)

etl_task >> feature_task >> forecast_task
