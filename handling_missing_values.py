from pyspark.sql import SparkSession

from spark_dataframe_operations import df_pyspark

spark = SparkSession.builder.appName("python-spark").getOrCreate()

# reading csv file
df_pyspark = spark.read.option("header", "true").csv("./data/test_data_3.csv", inferSchema=True)
df_pyspark.show()

# drop the columns
df_dropped_name = df_pyspark.drop('name')
df_dropped_name.show()

# drop rows that have null values
df_dropped_rows = df_pyspark.na.drop()
df_dropped_rows.show()

# how == all
df_dropped_row_all = df_pyspark.na.drop(how="all")
df_dropped_row_all.show()

# how == any (default)
df_dropped_row_any = df_pyspark.na.drop(how="any")
df_dropped_row_any.show()

# threshold -> at least two non-null values must be present
df_dropped_row_threshold = df_pyspark.na.drop(how="any", thresh=3)
df_dropped_row_threshold.show()

# subset
df_dropped_column_with_null = df_pyspark.na.drop(how="any", subset=['experience'])
df_dropped_column_with_null.show()

# Filling the missing value
df_filled_column_with_null = df_pyspark.na.fill('missing value')
df_filled_column_with_null.show()

df_filled_age_with_null = df_pyspark.na.fill('missing value', ['age'])
df_filled_age_with_null.show()

# replacing null with mean using imputer function
from pyspark.ml.feature import Imputer
imputer = Imputer(
    inputCols=['age', 'experience'],
    outputCols=["{}_imputed".format(c) for c in ['age', 'experience']]
).setStrategy("mean") # median

imputer.fit(df_pyspark).transform(df_pyspark).show()

