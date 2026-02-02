# 1. Make directory.
hdfs dfs -mkdir /user/amaan/bigdata_project
hdfs dfs -mkdir /user/amaan/bigdata_project/raw
hdfs dfs -mkdir -p /user/hive/warehouse/myprojectdb.db/cleaned_data
hdfs dfs -mkdir -p /user/hive/warehouse/myprojectdb.db/features
hdfs dfs -mkdir -p /user/hive/warehouse/myprojectdb.db/forecast

# 2. Dump the data in hdfs.
hdfs dfs -put /home/amaan/Desktop/archive/train_data.csv /user/amaan/bigdata_project/raw/
hdfs dfs -put /home/amaan/Desktop/archive/test_data.csv /user/amaan/bigdata_project/raw/