from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("python-spark").getOrCreate()
# df_pyspark=spark.read.csv("./data/test_data.csv")
df_pyspark = spark.read.option("header", "true").csv("./data/test_data.csv")
df_pyspark.show()

print(df_pyspark)
print(df_pyspark.head(2))
df_pyspark.printSchema()