from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("python-spark").getOrCreate()
# df_pyspark=spark.read.csv("./data/test_data.csv")
df_pyspark = spark.read.option("header", "true").csv("./data/test_data_2.csv", inferSchema=True)

# check schema, checking the data types of the column
# default column data type is String
df_pyspark.printSchema()

# read csv option 2
df_pyspark = spark.read.csv("./data/test_data_2.csv", header=True, inferSchema=True, sep=', ')
df_pyspark.printSchema()
print(type(df_pyspark))

# to get the first 2 records - list format
print(df_pyspark.head(2))

#df_pyspark.show()
# to get values of a specific column
# df_col_name = df_pyspark.select("name")
# df_col_name.show()

# to select multiple columns
df_col_name_exp = df_pyspark.select('name', 'experience')
df_col_name_exp.show()

print(df_pyspark.name)
# check data types
print(df_pyspark.dtypes)

# check describe
df_pyspark.describe().show()

# Adding columns in dataframe
df_col_new_exp = df_pyspark.withColumn('experience after 2 years', df_pyspark['experience']+2)
df_col_new_exp.show()

df_col_new_exp.select('name', 'experience after 2 years').show()


# Dropping a column
df_col_new_exp = df_col_new_exp.drop('experience after 2 years')
df_col_new_exp.show()


# renaming a column


