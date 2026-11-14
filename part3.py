from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "2")
    .appName("MyGoitSparkSandbox")
    .getOrCreate()
)

nuek_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("./nuek-vuh3.csv")
)

nuek_repart = nuek_df.repartition(2)

nuek_processed_cached = (
    nuek_repart
    .where("final_priority < 3")
    .select("unit_id", "final_priority")
    .groupBy("unit_id")
    .count()
    .cache()
)

# Перша дія обчислює результат і заповнює кеш
nuek_processed_cached.collect()

nuek_processed = nuek_processed_cached.where("count > 2")
nuek_processed.collect()

input("Перевірте SparkUI, потім натисніть Enter для завершення...")

nuek_processed_cached.unpersist()
spark.stop()