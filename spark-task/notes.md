

# Spark Task Notes

These notes summarize the implementation choices, observed results, and technical considerations for the PySpark portion of the project.

---

## Environment

```text
Ubuntu on WSL
Python 3.12
PySpark 4.2.0
Java 17
Spark local mode
```

Activate the Spark virtual environment with:

```bash
source /home/hadoop/.venvs/spark/bin/activate
```

---

## Dataset

Windows location:

```text
E:\enterprise-camp\data\D184MB\D184MB
```

WSL location:

```text
/mnt/e/enterprise-camp/data/D184MB/D184MB
```

Verified book count:

```text
425
```

Verification command:

```bash
find /mnt/e/enterprise-camp/data/D184MB/D184MB -type f -name "*.txt" | wc -l
```

Spark reads recursively from:

```text
file:///mnt/e/enterprise-camp/data/D184MB
```

Using `file:///` ensures Spark treats the path as a local filesystem location rather than resolving it through the Hadoop default filesystem.

---

# Dataset Loading

The shared loader creates a DataFrame with:

```text
file_name : string
text      : string
```

Each complete text file is loaded as one book record.

Run:

```bash
cd /mnt/e/enterprise-camp/spark-task/src
spark-submit test_load_books.py
```

Observed total:

```text
425 books
```

## Dataset Load Screenshot

![books_df loaded successfully](../doc/screenshots/spark/01-books-df-loaded.png)

---

# Q10 — Metadata Extraction

Run:

```bash
spark-submit q10_metadata.py
```

Extracted fields:

```text
title
release_date
language
encoding
release_year
```

The metadata is extracted from Gutenberg header text using regular expressions.

A safe cast is used for release years so invalid or missing values become `NULL` instead of causing the Spark job to fail.

## Extracted Metadata

![Q10 extracted metadata](../doc/screenshots/spark/02-q10-extracted-metadata.png)

## Release-Year Distribution

![Q10 release-year analysis](../doc/screenshots/spark/03-q10-analysis-results.png)

```text
1975 = 1
1978 = 1
1979 = 1
1991 = 7
1992 = 19
1993 = 13
1994 = 17
1995 = 60
1996 = 53
2002 = 1
2004 = 7
2005 = 4
2006 = 42
2007 = 13
2008 = 154
2009 = 1
2010 = 9
2011 = 1
2012 = 2
2013 = 1
2015 = 1
```

## Language Distribution

```text
English = 404
Latin   = 6
```

One malformed extracted value appeared:

```text
and thus we see how, little by little, the study of man = 1
```

This is a useful example of noisy source metadata and shows that regex-based extraction can occasionally match text that is not actually a metadata field value.

## Average Title Length

Exact value:

```text
22.023529411764706
```

Rounded:

```text
22.02 characters
```

## Q10 Observations

The extraction approach works well when Gutenberg headers follow the expected format, but metadata quality is not perfectly consistent.

Potential issues include:

- missing fields
- inconsistent formatting
- unexpected capitalization
- extra whitespace
- malformed release dates
- false-positive regex matches

For a larger production workflow, the metadata should be validated and normalized before analysis.

---

# Q11 — TF-IDF and Cosine Similarity

Run:

```bash
spark-submit --driver-memory 2g q11_tfidf.py
```

## Processing Pipeline

```text
raw book text
→ remove Gutenberg header/footer
→ lowercase
→ remove punctuation
→ tokenize
→ remove stop words
→ CountVectorizer
→ TF
→ IDF
→ TF-IDF
→ L2 normalization
→ cosine similarity
```

## TF

TF, or Term Frequency, measures how often a term appears in a particular document.

## IDF

IDF, or Inverse Document Frequency, reduces the importance of terms that appear in many documents and gives relatively greater weight to rarer terms.

## TF-IDF

TF-IDF combines within-document frequency and across-document rarity to create a weighted vector representation for each book.

## Cosine Similarity

Cosine similarity compares the direction of two TF-IDF vectors.

Interpretation:

```text
closer to 1 → more similar
closer to 0 → less similar
```

Because the vectors are normalized, their dot product gives the cosine similarity value.

## Q11 Results

```text
Number of books: 425
Vocabulary size: 50000
Total unique book pairs compared: 90100
```

Pair-count check:

```text
425 × 424 / 2 = 90100
```

## Top 5 Books Similar to 10.txt

```text
58.txt   = 0.406189
30.txt   = 0.398651
26.txt   = 0.375180
357.txt  = 0.299850
109.txt  = 0.284691
```

## Top Similarity Pairs Observed

```text
37.txt  ↔ 29.txt   = 0.999842
463.txt ↔ 73.txt   = 0.999382
221.txt ↔ 108.txt  = 0.998203
107.txt ↔ 27.txt   = 0.997499
201.txt ↔ 97.txt   = 0.995184
129.txt ↔ 127.txt  = 0.981001
361.txt ↔ 362.txt  = 0.942719
48.txt  ↔ 25.txt   = 0.929655
2.txt   ↔ 4.txt    = 0.929174
87.txt  ↔ 48.txt   = 0.922088
```

Saved output:

```text
spark-task/results/q11/all_pair_similarities.csv
```

## Q11 Output Screenshot

![Q11 TF-IDF and cosine similarity results](../doc/screenshots/spark/04-q11-tfidf-cosine-results.png)

## Q11 Scalability Notes

The number of unique pairwise comparisons is:

```text
n(n - 1) / 2
```

so the workload grows approximately as:

```text
O(n²)
```

For a much larger dataset, calculating every possible pair would become expensive.

Spark helps distribute preprocessing and vector construction, but full pairwise comparison is still costly at scale.

Possible alternatives for very large collections include:

- approximate nearest-neighbour search
- locality-sensitive hashing
- dimensionality reduction
- top-k search instead of full all-pairs comparison

---

# Q12 — Author Influence Network

Run:

```bash
spark-submit --driver-memory 2g q12_author_network.py
```

## Influence Rule

A directed edge:

```text
author1 → author2
```

is created when:

```text
year2 > year1
```

and:

```text
year2 - year1 <= 5
```

Influence window:

```text
X = 5 years
```

Before generating edges, duplicate `(author, release_year)` records are removed.

## Q12 Results

```text
Total books: 425
Valid book records with author/year: 407
Unique author-year records: 309
Number of network edges: 12241
```

## Top 5 Authors by Out-Degree

```text
Edgar Rice Burroughs    = 206
Charles Dickens         = 193
Lucy Maud Montgomery    = 192
Robert Louis Stevenson  = 192
Thomas Hardy            = 192
```

## Top 5 Authors by In-Degree

```text
Robert Louis Stevenson = 142
J. M. Barrie           = 133
Jerome K. Jerome       = 133
Anonymous              = 132
Arthur Conan Doyle     = 129
```

## Q12 Output Screenshot

![Q12 author influence network results](../doc/screenshots/spark/05-q12-author-network-results.png)

## Q12 Representation Notes

The network is represented as a Spark DataFrame of directed author pairs:

```text
(author1, author2)
```

DataFrames are convenient for:

- filtering
- joins
- grouping
- aggregation
- schema-based processing
- Spark SQL operations
- optimized execution

An RDD could also represent the network, but DataFrames are more natural for this structured analysis.

## Effect of Changing X

Smaller `X`:

```text
fewer edges
sparser network
lower degree values
```

Larger `X`:

```text
more edges
denser network
higher degree values
```

The choice of time window therefore directly changes the shape and density of the graph.

## Q12 Limitations

The five-year publication rule is only a simplified temporal proxy for influence.

It does not prove that one author influenced another.

The method does not account for:

- genre
- language
- geography
- historical context
- citations
- correspondence
- direct references
- whether one author had actually read another author's work

The presence of values such as `Anonymous` also indicates that author metadata may require normalization and entity cleaning in a more rigorous analysis.

## Q12 Scalability Notes

The current implementation uses a cross join to generate candidate author pairs.

For millions of records, that approach would become expensive.

More scalable approaches include:

- filter invalid records early
- deduplicate author-year records before joins
- partition by release year
- join only nearby year ranges
- cache reused DataFrames
- use graph-processing frameworks for larger networks
