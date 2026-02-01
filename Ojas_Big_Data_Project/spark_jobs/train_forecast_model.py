from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline

# -------------------------------------------------
# 1. Spark Session with Hive Support
# -------------------------------------------------
spark = SparkSession.builder \
    .appName("Train Sales Forecast Model") \
    .enableHiveSupport() \
    .getOrCreate()

# -------------------------------------------------
# 2. Read Spark-readable Hive table (NOT MV)
# -------------------------------------------------
df = spark.sql("""
    SELECT
        `date`,
        store_nbr,
        family,
        total_sales,
        total_transactions,
        avg_oil_price
    FROM project.train_features
""")

# -------------------------------------------------
# 3. Time-based Feature Engineering
# -------------------------------------------------
df = df.withColumn("year", year(col("date"))) \
       .withColumn("month", month(col("date")))

# -------------------------------------------------
# 4. Encode Categorical Column
# -------------------------------------------------
family_indexer = StringIndexer(
    inputCol="family",
    outputCol="family_index",
    handleInvalid="keep"
)

# -------------------------------------------------
# 5. Assemble Features
# -------------------------------------------------
assembler = VectorAssembler(
    inputCols=[
        "store_nbr",
        "family_index",
        "total_transactions",
        "avg_oil_price",
        "year",
        "month"
    ],
    outputCol="features"
)

# -------------------------------------------------
# 6. Define Regression Model
# -------------------------------------------------
lr = LinearRegression(
    featuresCol="features",
    labelCol="total_sales"
)

# -------------------------------------------------
# 7. Pipeline
# -------------------------------------------------
pipeline = Pipeline(stages=[
    family_indexer,
    assembler,
    lr
])

# -------------------------------------------------
# 8. Train Model
# -------------------------------------------------
model = pipeline.fit(df)

# -------------------------------------------------
# 9. Save Model to HDFS
# -------------------------------------------------
model.write().overwrite().save(
    "hdfs://localhost:9000/user/project/retail/models/sales_forecast_lr"
)

print("✅ Model training completed successfully")

spark.stop()
