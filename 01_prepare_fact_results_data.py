from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("Create test table from fact_results").config(
    "spark.jars", "lib/postgresql-42.3.6.jar").getOrCreate()

# spark-submit --driver-class-path lib/postgresql-42.3.6.jar 01_prepare_fact_results_data.py

# Postgres connection variable
POSTGRES_URL = "jdbc:postgresql://localhost:5432/segunda_division_dw"
TABLE = "fact_results"
TABLE_TEST = "fact_results_test"
USERNAME = "airflow"
PASSWORD = "airflow"
DRIVER = "org.postgresql.Driver"

df = spark.read.format("jdbc").option("url", POSTGRES_URL).option("driver", DRIVER).option("dbtable", TABLE).option(
    "user", USERNAME).option("password", PASSWORD).load()

start_count = df.count()
print(start_count)

for a in range(0, 11):
    df = df.union(df)

end_count = df.count()
print(end_count)

df.write.format('jdbc').option("url", POSTGRES_URL).option("driver", DRIVER).option("dbtable", TABLE_TEST).option(
        "user", USERNAME).option("password", PASSWORD).save()
