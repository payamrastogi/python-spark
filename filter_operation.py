from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("python-spark").getOrCreate()

# reading csv file
df_pyspark = spark.read.option("header", "true").csv("./data/test_data_4.csv", inferSchema=True)
df_pyspark.show()

# Filter operations

# persons having Salary less than 20000

df_filtered_based_on_salary = df_pyspark.filter("salary<=20000").select(['name', 'age', 'salary'])
df_filtered_based_on_salary.show()

# multiple conditions in filter
df_filtered_multi = df_pyspark.filter((df_pyspark['salary']<=20000)
                                       & (df_pyspark['salary']>15000))
df_filtered_multi.show()

# not condition
df_filtered_not = df_pyspark.filter(~(df_pyspark['salary']<=20000)).select(['name', 'age', 'salary'])
df_filtered_not.show()
