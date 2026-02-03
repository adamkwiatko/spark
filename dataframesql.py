from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrameSQL").getOrCreate()

csv_file_path = "./data/persons.csv"

df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

df.printSchema()
df.show()

df.createOrReplaceTempView("my_table")

result = spark.sql("SELECT * FROM my_table WHERE age > 25")

result.show()


vg_salary_by_gender = spark.sql("SELECT gender, AVG(salary) as avg_salary FROM my_table GROUP BY gender")
vg_salary_by_gender.show()

views_exists = spark.catalog.tableExists("my_table")
print(views_exists)
