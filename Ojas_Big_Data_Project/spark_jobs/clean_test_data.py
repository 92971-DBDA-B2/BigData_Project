from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_date, avg


spark = SparkSession.builder \
    .appName("Clean Test Data") \
    .getOrCreate()

# ---------------- PATHS ---------------- #

# Raw TEST data
test_input_path = "hdfs://localhost:9000/user/project/retail/raw/test/test_data.csv"

# Cleaned TRAIN data (used ONLY to get oil average)
train_cleaned_path = "hdfs://localhost:9000/user/project/retail/cleaned/train"

# Output path for cleaned TEST data
test_output_path = "hdfs://localhost:9000/user/project/retail/cleaned/test"

# ---------------- SCHEMA (SAME AS TRAIN) ---------------- #

data_schema = StructType([
    StructField("date", StringType(), True),
    StructField("store_nbr", IntegerType(), True),
    StructField("family", StringType(), True),
    StructField("sales", DoubleType(), True),          # may or may not exist in test
    StructField("onpromotion", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("type_of_store", StringType(), True),
    StructField("cluster", IntegerType(), True),
    StructField("dcoilwtico", DoubleType(), True),
    StructField("transactions", IntegerType(), True),
    StructField("n_holidays", IntegerType(), True)
])

# ---------------- READ TEST DATA ---------------- #

test_df = spark.read \
    .option("header", "true") \
    .schema(data_schema) \
    .csv(test_input_path)

# ---------------- GET OIL AVG FROM TRAIN (NO LEAKAGE) ---------------- #

train_clean_df = spark.read.parquet(train_cleaned_path)
oil_avg = train_clean_df.select(avg("dcoilwtico")).first()[0]

# ---------------- DATA CLEANING (TEST) ---------------- #

# 1. Handle missing transactions
test_df = test_df.fillna({"transactions": 0})

# 2. Handle missing oil price using TRAIN average
test_df = test_df.fillna({"dcoilwtico": oil_avg})

# 3. Handle other numeric nulls
test_df = test_df.fillna({
    "onpromotion": 0,
    "cluster": 0,
    "n_holidays": 0
})

# (sales may not exist in test — if it does, do NOT use it for learning)
test_df = test_df.fillna({"sales": 0})

# 4. Convert date to DateType
test_df = test_df.withColumn(
    "date",
    to_date(col("date"), "yyyy-MM-dd")
)

# 5. Drop rows with invalid dates
test_df = test_df.filter(col("date").isNotNull())

# 6. Remove duplicate rows
test_df = test_df.dropDuplicates()

# 7. Validate numeric ranges
test_df = test_df.filter(
    (col("onpromotion") >= 0) &
    (col("transactions") >= 0)
)

# 8. Handle categorical nulls
test_df = test_df.fillna({
    "family": "UNKNOWN",
    "city": "UNKNOWN",
    "type_of_store": "UNKNOWN"
})

# ---------------- CHECK RESULT ---------------- #

test_df.printSchema()
test_df.show(5)

# ---------------- WRITE CLEANED TEST DATA ---------------- #

test_df.write \
    .mode("overwrite") \
    .parquet(test_output_path)

# Stop Spark
spark.stop()
