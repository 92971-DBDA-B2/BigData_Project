from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month
from pyspark.ml import PipelineModel

# -------------------------------------------------
# 1. Spark Session
# -------------------------------------------------
spark = SparkSession.builder \
    .appName("Sales Forecasting") \
    .enableHiveSupport() \
    .getOrCreate()

# -------------------------------------------------
# 2. Load Trained Model
# -------------------------------------------------
model = PipelineModel.load(
    "hdfs://localhost:9000/user/project/retail/models/sales_forecast_lr"
)

# -------------------------------------------------
# 3. Read Clean Test Data from Hive
# -------------------------------------------------
future_df = spark.sql("""
    SELECT
        `date`,
        store_nbr,
        family,
        transactions AS total_transactions,
        dcoilwtico AS avg_oil_price
    FROM project.test_cleaned
""")

# -------------------------------------------------
# 4. Time Features (MUST match training)
# -------------------------------------------------
future_df = future_df.withColumn("year", year("date")) \
                     .withColumn("month", month("date"))

# -------------------------------------------------
# 5. Predict
# -------------------------------------------------
predictions = model.transform(future_df)

# -------------------------------------------------
# 6. Save Forecast Output
# -------------------------------------------------
predictions.select(
    "date",
    "store_nbr",
    "family",
    "prediction"
).write.mode("overwrite").parquet(
    "hdfs://localhost:9000/user/project/retail/forecast/output"
)

print("✅ Sales forecast generated successfully")

spark.stop()
