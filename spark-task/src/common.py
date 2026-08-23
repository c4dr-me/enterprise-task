from pyspark.sql import functions as F


def load_books(spark, path):
    return (
        spark.read
        .format("binaryFile")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.txt")
        .load(path)
        .select(
            F.regexp_extract(F.col("path"), r"([^/]+)$", 1).alias("file_name"),
            F.decode(F.col("content"), "UTF-8").alias("text")
        )
    )
