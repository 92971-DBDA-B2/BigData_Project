from pyspark.sql import SparkSession

# create a spark session 

spark = SparkSession.builder.appName("Read Train Data").getOrCreate()

filepath = "hdfs://localhost:9000/user/project/retail/raw/train/train_data.csv"

train_df = spark.read\
            .option("header","true")\
            .option("inferschema","true")\
            .csv(filepath)

train_df.show(5)
train_df.printSchema()




spark.stop()

