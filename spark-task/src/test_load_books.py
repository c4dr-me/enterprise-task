from pyspark.sql import SparkSession
from common import load_books

print("SCRIPT STARTED")

spark = (
    SparkSession.builder
    .appName("Load Gutenberg Books")
    .master("local[*]")
    .getOrCreate()
)

print("SPARK SESSION CREATED")

books_df = load_books(
    spark,
    "file:///mnt/e/enterprise-camp/data/D184MB"
)

books_df.printSchema()

print("Number of books:", books_df.count())

books_df.select("file_name").show(10, truncate=False)

spark.stop()
