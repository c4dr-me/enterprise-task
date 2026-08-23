from pyspark.sql import SparkSession, functions as F

from pyspark.ml.feature import (
    RegexTokenizer,
    StopWordsRemover,
    CountVectorizer,
    IDF,
    Normalizer
)

from common import load_books

import os
import csv


# --------------------------------------------------
# Spark setup
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Q11 TF-IDF Book Similarity")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# --------------------------------------------------
# Load books
# --------------------------------------------------

books_df = load_books(
    spark,
    "file:///mnt/e/enterprise-camp/data/D184MB"
)


# --------------------------------------------------
# 1. Preprocessing
# --------------------------------------------------

# Remove Project Gutenberg header/footer
clean_df = (
    books_df
    .withColumn(
        "text",
        F.regexp_replace(
            "text",
            r"(?is)^.*?\*\*\*\s*START OF .*?\*\*\*",
            ""
        )
    )
    .withColumn(
        "text",
        F.regexp_replace(
            "text",
            r"(?is)\*\*\*\s*END OF .*?$",
            ""
        )
    )
)

# Lowercase and remove punctuation
clean_df = (
    clean_df
    .withColumn(
        "text",
        F.lower("text")
    )
    .withColumn(
        "text",
        F.regexp_replace(
            "text",
            r"[^a-z0-9\s]",
            " "
        )
    )
)


# --------------------------------------------------
# 2. Tokenization
# --------------------------------------------------

tokenizer = RegexTokenizer(
    inputCol="text",
    outputCol="tokens",
    pattern=r"\s+",
    minTokenLength=2
)

tokenized_df = tokenizer.transform(clean_df)


# --------------------------------------------------
# 3. Remove stop words
# --------------------------------------------------

remover = StopWordsRemover(
    inputCol="tokens",
    outputCol="words"
)

words_df = remover.transform(tokenized_df)


# --------------------------------------------------
# 4. Term Frequency (TF)
# --------------------------------------------------

count_vectorizer = CountVectorizer(
    inputCol="words",
    outputCol="tf",
    vocabSize=50000
)

cv_model = count_vectorizer.fit(words_df)

tf_df = cv_model.transform(words_df)


# --------------------------------------------------
# 5. Inverse Document Frequency (IDF)
# --------------------------------------------------

idf = IDF(
    inputCol="tf",
    outputCol="tfidf"
)

idf_model = idf.fit(tf_df)

tfidf_df = idf_model.transform(tf_df)


# --------------------------------------------------
# 6. Normalize TF-IDF vectors
# --------------------------------------------------
# After L2 normalization:
# cosine similarity = vector1 dot vector2
# --------------------------------------------------

normalizer = Normalizer(
    inputCol="tfidf",
    outputCol="features",
    p=2.0
)

vectors_df = (
    normalizer
    .transform(tfidf_df)
    .select(
        "file_name",
        "features"
    )
    .cache()
)


# Execute Spark work before printing final results
book_count = vectors_df.count()
vocabulary_size = len(cv_model.vocabulary)

books = vectors_df.collect()


# --------------------------------------------------
# 7. Cosine similarity between ALL book pairs
# --------------------------------------------------

pair_results = []

for i in range(len(books)):
    for j in range(i + 1, len(books)):

        book1 = books[i]
        book2 = books[j]

        similarity = float(
            book1["features"].dot(book2["features"])
        )

        pair_results.append(
            (
                book1["file_name"],
                book2["file_name"],
                similarity
            )
        )


# Highest similarity first
pair_results.sort(
    key=lambda row: row[2],
    reverse=True
)


# --------------------------------------------------
# 8. Find top 5 books similar to 10.txt
# --------------------------------------------------

target_file = "10.txt"

target_results = []

for book1, book2, similarity in pair_results:

    if book1 == target_file:
        target_results.append(
            (book2, similarity)
        )

    elif book2 == target_file:
        target_results.append(
            (book1, similarity)
        )


target_results.sort(
    key=lambda row: row[1],
    reverse=True
)

top_5_target = target_results[:5]


# --------------------------------------------------
# 9. Save ALL pairwise similarities
# --------------------------------------------------

results_dir = (
    "/mnt/e/enterprise-camp/"
    "spark-task/results/q11"
)

os.makedirs(
    results_dir,
    exist_ok=True
)

csv_path = os.path.join(
    results_dir,
    "all_pair_similarities.csv"
)

with open(
    csv_path,
    "w",
    newline="",
    encoding="utf-8"
) as file:

    writer = csv.writer(file)

    writer.writerow(
        [
            "book1",
            "book2",
            "cosine_similarity"
        ]
    )

    writer.writerows(pair_results)


# --------------------------------------------------
# 10. FINAL OUTPUT
# --------------------------------------------------

print("\n")
print("=" * 60)
print("Q11 - TF-IDF AND BOOK SIMILARITY RESULTS")
print("=" * 60)

print(
    f"\nNumber of books: {book_count}"
)

print(
    f"Vocabulary size: {vocabulary_size}"
)

print(
    f"Total book pairs compared: "
    f"{len(pair_results)}"
)


print("\nTOP 5 BOOKS MOST SIMILAR TO 10.txt")
print("-" * 60)

print(
    f"{'Book':<15}"
    f"{'Cosine Similarity':>20}"
)

for file_name, similarity in top_5_target:

    print(
        f"{file_name:<15}"
        f"{similarity:>20.6f}"
    )


print("\nTOP 10 MOST SIMILAR PAIRS IN THE DATASET")
print("-" * 60)

print(
    f"{'Book 1':<15}"
    f"{'Book 2':<15}"
    f"{'Similarity':>15}"
)

for book1, book2, similarity in pair_results[:10]:

    print(
        f"{book1:<15}"
        f"{book2:<15}"
        f"{similarity:>15.6f}"
    )


print("\nAll pairwise similarities saved to:")
print(csv_path)

print("=" * 60)


vectors_df.unpersist()
spark.stop()