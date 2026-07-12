from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.table("customer")

df = df.select(
    "customer_id",
    "customer_name",
    "email"
)

df.write.mode("overwrite").saveAsTable("customer_gold")
