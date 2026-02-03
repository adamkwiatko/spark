from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("spark-sql").getOrCreate()

employee_data = [
        (1, "John"), (2, "Alice"), (3, "Bob"), (4, "Emily"),
        (5, "David"), (6, "Sarah"), (7, "Michael"), (8, "Lisa"),
        (9, "William")
]

employees = spark.createDataFrame(employee_data, ["id", "name"])

salary_data = [
        ("HR", 1, 60000), ("HR", 2, 55000), ("HR", 3, 58000),
        ("IT", 4, 70000), ("IT", 5, 72000), ("IT", 6, 68000),
        ("Sales", 7, 75000), ("Sales", 8, 78000), ("Sales", 9, 77000)
]

salaries = spark.createDataFrame(salary_data, ["department", "id", "salary"])

employees.show()
salaries.show()


employees.createOrReplaceTempView("employees")
salaries.createOrReplaceTempView("salaries")

result = spark.sql("""
                   SELECT name
                   FROM employees
                   WHERE id IN (
                        SELECT id
                        FROM salaries
                        WHERE salary > (SELECT AVG(salary) FROM salaries)
                        )
                """)

result.show()

employee_salary = spark.sql("""
                           select salaries.*, employees.name
                           from salaries
                           left join employees on salaries.id = employees.id
                           """)

employee_salary.show()

window_spec = Window.partitionBy("department").orderBy(F.desc("salary"))

employee_salary.withColumn("rank", F.rank().over(window_spec)).show()

spark.stop()
