from pyspark.sql import SparkSession, functions as F
from common import load_books


spark = (
    SparkSession.builder
    .appName("Q10 Metadata Analysis")
    .master("local[*]")
    .getOrCreate()
)

books_df = load_books(
    spark,
    "file:///mnt/e/enterprise-camp/data/D184MB"
)

metadata_df = (
    books_df
    .withColumn(
        "title",
        F.regexp_extract("text", r"(?im)^Title:\s*(.+)$", 1)
    )
    .withColumn(
        "release_date",
        F.regexp_extract("text", r"(?im)^Release Date:\s*(.+)$", 1)
    )
    .withColumn(
        "language",
        F.regexp_extract("text", r"(?im)^Language:\s*(.+)$", 1)
    )
    .withColumn(
        "encoding",
        F.regexp_extract(
            "text",
            r"(?im)^(?:Character Set Encoding|Encoding):\s*(.+)$",
            1
        )
    )
    .withColumn(
    "release_year",
    F.expr(
        "try_cast(regexp_extract(release_date, '(\\\\d{4})', 1) AS INT)"
        )
    )
)

print("\nEXTRACTED METADATA")

metadata_df.select(
    "file_name",
    "title",
    "release_date",
    "language",
    "encoding"
).show(20, truncate=60)


print("\nBOOKS RELEASED EACH YEAR")

(
    metadata_df
    .filter(F.col("release_year").isNotNull())
    .groupBy("release_year")
    .count()
    .orderBy("release_year")
    .show(100)
)


print("\nMOST COMMON LANGUAGE")

(
    metadata_df
    .filter(F.col("language") != "")
    .groupBy("language")
    .count()
    .orderBy(F.desc("count"))
    .show(10, truncate=False)
)


print("\nAVERAGE TITLE LENGTH")

metadata_df.select(
    F.avg(F.length("title")).alias("average_title_length")
).show()


spark.stop()