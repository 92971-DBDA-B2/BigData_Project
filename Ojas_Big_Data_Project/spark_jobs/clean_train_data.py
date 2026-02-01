from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_date, avg


spark = SparkSession.builder \
    .appName("Clean Train Data") \
    .getOrCreate()

# HDFS path
filepath = "hdfs://localhost:9000/user/project/retail/raw/train/train_data.csv"

# Define schema (NOTE: date as StringType initially)
data_schema = StructType([
    StructField("date", StringType(), True),
    StructField("store_nbr", IntegerType(), True),
    StructField("family", StringType(), True),
    StructField("sales", DoubleType(), True),
    StructField("onpromotion", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("type_of_store", StringType(), True),
    StructField("cluster", IntegerType(), True),
    StructField("dcoilwtico", DoubleType(), True),
    StructField("transactions", IntegerType(), True),
    StructField("n_holidays", IntegerType(), True)
])

# Read CSV
train_df = spark.read \
    .option("header", "true") \
    .schema(data_schema) \
    .csv(filepath)

# ---------------- DATA CLEANING ---------------- #

# 1. Handle missing transactions
train_df = train_df.fillna({"transactions": 0})

# 2. Handle missing oil price using TRAIN average
oil_avg = train_df.select(avg("dcoilwtico")).first()[0]
train_df = train_df.fillna({"dcoilwtico": oil_avg})

# 3. Handle other numeric nulls
train_df = train_df.fillna({
    "sales": 0,
    "onpromotion": 0,
    "cluster": 0,
    "n_holidays": 0
})

# 4. Convert date to DateType
train_df = train_df.withColumn(
    "date",
    to_date(col("date"), "yyyy-MM-dd")
)

# 5. Drop rows with invalid dates
train_df = train_df.filter(col("date").isNotNull())

# 6. Remove duplicate rows
train_df = train_df.dropDuplicates()

# 7. Validate numeric ranges
train_df = train_df.filter(
    (col("sales") >= 0) &
    (col("onpromotion") >= 0) &
    (col("transactions") >= 0)
)

# 8. Handle categorical nulls
train_df = train_df.fillna({
    "family": "UNKNOWN",
    "city": "UNKNOWN",
    "type_of_store": "UNKNOWN"
})

# ---------------- CHECK RESULT ---------------- #

train_df.printSchema()
train_df.show(5)

train_df.write \
    .mode("overwrite") \
    .parquet("hdfs://localhost:9000/user/project/retail/cleaned/train")


# Stop Spark
spark.stop()
