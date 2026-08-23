from pyspark.sql import SparkSession, functions as F

from common import load_books


# ============================================================
# 1. Spark setup
# ============================================================

spark = (
    SparkSession.builder
    .appName("Q12 Author Influence Network")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# 2. Load Gutenberg books
# ============================================================

books_df = load_books(
    spark,
    "file:///mnt/e/enterprise-camp/data/D184MB"
)

total_books = books_df.count()


# ============================================================
# 3. Extract author and release date
# ============================================================

metadata_df = (
    books_df
    .withColumn(
        "author",
        F.trim(
            F.regexp_extract(
                "text",
                r"(?im)^Author:\s*(.+)$",
                1
            )
        )
    )
    .withColumn(
        "release_date",
        F.trim(
            F.regexp_extract(
                "text",
                r"(?im)^Release Date:\s*(.+)$",
                1
            )
        )
    )
)


# ============================================================
# 4. Extract release year safely
# ============================================================

metadata_df = metadata_df.withColumn(
    "release_year",
    F.expr(
        "try_cast("
        "regexp_extract(release_date, '(\\\\d{4})', 1) "
        "AS INT)"
    )
)


# ============================================================
# 5. Keep only valid author/year records
# ============================================================

valid_df = (
    metadata_df
    .filter(
        (F.col("author") != "") &
        F.col("release_year").isNotNull()
    )
    .select(
        "file_name",
        "author",
        "release_year"
    )
)

valid_book_count = valid_df.count()


# ============================================================
# 6. Create unique author-year records
# ============================================================
# This avoids creating duplicate candidate relationships
# when the same author appears multiple times in the same year.
# ============================================================

author_year_df = (
    valid_df
    .select(
        "author",
        "release_year"
    )
    .dropDuplicates()
)

author_year_count = author_year_df.count()


# ============================================================
# 7. Influence rule
# ============================================================
# author1 -> author2 if:
#
# year2 > year1
# AND
# year2 - year1 <= X
#
# X = 5 years
# ============================================================

X = 5


# ============================================================
# 8. Prepare left and right sides
# ============================================================

a = author_year_df.select(
    F.col("author").alias("author1"),
    F.col("release_year").alias("year1")
)

b = author_year_df.select(
    F.col("author").alias("author2"),
    F.col("release_year").alias("year2")
)


# ============================================================
# 9. Create directed influence edges
# ============================================================

edges_df = (
    a.crossJoin(b)
    .filter(
        (F.col("author1") != F.col("author2")) &
        (F.col("year2") > F.col("year1")) &
        ((F.col("year2") - F.col("year1")) <= X)
    )
    .select(
        "author1",
        "author2"
    )
    .dropDuplicates()
    .cache()
)

edge_count = edges_df.count()


# ============================================================
# 10. Calculate out-degree
# ============================================================
# Out-degree:
# number of authors that author1 potentially influenced
# ============================================================

out_degree_df = (
    edges_df
    .groupBy("author1")
    .agg(
        F.countDistinct("author2")
        .alias("out_degree")
    )
    .orderBy(
        F.desc("out_degree"),
        F.asc("author1")
    )
)


# ============================================================
# 11. Calculate in-degree
# ============================================================
# In-degree:
# number of authors that potentially influenced author2
# ============================================================

in_degree_df = (
    edges_df
    .groupBy("author2")
    .agg(
        F.countDistinct("author1")
        .alias("in_degree")
    )
    .orderBy(
        F.desc("in_degree"),
        F.asc("author2")
    )
)


# ============================================================
# 12. Final output
# ============================================================

print("\n")
print("=" * 60)
print("Q12 - AUTHOR INFLUENCE NETWORK RESULTS")
print("=" * 60)

print(f"\nTotal books: {total_books}")
print(f"Valid book records with author/year: {valid_book_count}")
print(f"Unique author-year records: {author_year_count}")
print(f"Influence window X: {X} years")
print(f"Number of network edges: {edge_count}")


print("\nSAMPLE INFLUENCE EDGES")
print("-" * 60)

edges_df.orderBy(
    "author1",
    "author2"
).show(
    10,
    truncate=False
)


print("\nTOP 5 AUTHORS BY OUT-DEGREE")
print("-" * 60)

out_degree_df.show(
    5,
    truncate=False
)


print("\nTOP 5 AUTHORS BY IN-DEGREE")
print("-" * 60)

in_degree_df.show(
    5,
    truncate=False
)


print("=" * 60)


# ============================================================
# 13. Cleanup
# ============================================================

edges_df.unpersist()
spark.stop()