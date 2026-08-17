# Hadoop Task Notes

## Environment

- OS: Ubuntu on WSL (Windows)
- Hadoop user: `hadoop`
- Hadoop version: `3.5.0`
- Java: OpenJDK `17.0.19`
- `javac`: `17.0.19`
- Project on Windows: `E:\enterprise-camp`
- Project in WSL: `/mnt/e/enterprise-camp`
- Input PDF: `grokking-algorithms-illustrated-programmers-curious.pdf`
- Converted text input: `book.txt`

## Local project structure

```text
E:\enterprise-camp
├── data
│   ├── grokking-algorithms-illustrated-programmers-curious.pdf
│   └── book.txt
├── doc
│   ├── notes.md
│   └── screenshots
└── hadoop-task
    ├── src
    │   └── WordCount.java
    ├── build
    └── results
        ├── builtin-wordcount.txt
        └── custom-wordcount.txt
```

## 1. SSH and Hadoop startup

Hadoop was installed under a dedicated `hadoop` user. Passwordless SSH to localhost was configured earlier.

When WSL was started, SSH initially returned:

```text
ssh: connect to host localhost port 22: Connection refused
```

The SSH server was started, and then this worked:

```bash
ssh localhost
```

Hadoop services were started with:

```bash
start-dfs.sh
start-yarn.sh
```

Verification:

```bash
jps
```

Observed Hadoop processes:

```text
NameNode
DataNode
SecondaryNameNode
ResourceManager
NodeManager
Jps
```

## 2. PDF to text conversion

The input PDF was stored in:

```text
/mnt/e/enterprise-camp/data/
```

`poppler-utils` was installed and the PDF was converted using:

```bash
cd /mnt/e/enterprise-camp

pdftotext \
  data/grokking-algorithms-illustrated-programmers-curious.pdf \
  data/book.txt
```

Verification commands:

```bash
ls -lh data/
head -20 data/book.txt
wc -l data/book.txt
wc -w data/book.txt
```

Observed local text statistics:

```text
book.txt size: approximately 242 KB
Lines reported by wc -l: 7366
Words reported by wc -w: 43251
```

The extracted text was readable, so no OCR step was needed.

## 3. Upload input to HDFS

Created an HDFS input directory:

```bash
hdfs dfs -mkdir -p /user/hadoop/hadoop-task/input
```

Uploaded `book.txt`:

```bash
hdfs dfs -put \
  /mnt/e/enterprise-camp/data/book.txt \
  /user/hadoop/hadoop-task/input/
```

Verified the upload:

```bash
hdfs dfs -ls /user/hadoop/hadoop-task/input
hdfs dfs -du -h /user/hadoop/hadoop-task/input/book.txt
```

Observed:

```text
-rw-r--r--   1 hadoop supergroup 247495 ... /user/hadoop/hadoop-task/input/book.txt
241.7 K  241.7 K  /user/hadoop/hadoop-task/input/book.txt
```

## 4. Built-in Hadoop WordCount

The built-in Hadoop WordCount example was run with:

```bash
hadoop jar \
  $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.5.0.jar \
  wordcount \
  /user/hadoop/hadoop-task/input \
  /user/hadoop/hadoop-task/builtin-wordcount
```

Output directory verification:

```bash
hdfs dfs -ls /user/hadoop/hadoop-task/builtin-wordcount
```

The job produced:

```text
_SUCCESS
part-r-00000
```

The first 50 output records were inspected with:

```bash
hdfs dfs -cat \
  /user/hadoop/hadoop-task/builtin-wordcount/part-r-00000 \
  | head -50
```

Example output showed punctuation was retained as part of tokens:

```text
#       3
#0,     1
$1,500. 7
(BFS).  1
```

This demonstrated a limitation of the built-in example for this dataset: values such as `algorithm`, `algorithm,`, and `algorithm.` may be treated as different keys.

The most common tokens were inspected with:

```bash
hdfs dfs -cat \
  /user/hadoop/hadoop-task/builtin-wordcount/part-r-00000 \
  | sort -k2,2nr \
  | head -20
```

The full reducer output was saved locally using shell redirection:

```bash
hdfs dfs -cat \
  /user/hadoop/hadoop-task/builtin-wordcount/part-r-00000 \
  > /mnt/e/enterprise-camp/hadoop-task/results/builtin-wordcount.txt
```

`hdfs dfs -getmerge` was attempted first but returned `Operation not permitted` when writing to the Windows-mounted destination, so direct redirection from `part-r-00000` was used instead.

Distinct output token forms:

```bash
wc -l /mnt/e/enterprise-camp/hadoop-task/results/builtin-wordcount.txt
```

Observed:

```text
6122
```

## 5. Mapper and Reducer data types

### Mapper input

Hadoop supplies each line to the Mapper as:

```text
(LongWritable, Text)
```

- `LongWritable`: byte offset of the record in the input file.
- `Text`: the line contents.

Example:

```text
(0, "grokking algorithms")
```

### Mapper output

The Mapper emits:

```text
(Text, IntWritable)
```

Example:

```text
("grokking", 1)
("algorithms", 1)
```

### Reducer input

After shuffle and sort, the Reducer receives:

```text
(Text, Iterable<IntWritable>)
```

Example:

```text
("algorithm", [1, 1, 1, 1, ...])
```

### Reducer output

The Reducer emits:

```text
(Text, IntWritable)
```

Example:

```text
("algorithm", 84)
```

## 6. Custom Java WordCount

The custom Java source was stored at:

```text
/mnt/e/enterprise-camp/hadoop-task/src/WordCount.java
```

Important Mapper behavior:

```java
String cleanLine = value.toString()
        .replaceAll("[^A-Za-z0-9\\s]", "");

StringTokenizer tokenizer = new StringTokenizer(cleanLine);
```

The custom Mapper removes punctuation before tokenization.

The Mapper type declaration is:

```java
Mapper<LongWritable, Text, Text, IntWritable>
```

The Reducer type declaration is:

```java
Reducer<Text, IntWritable, Text, IntWritable>
```

Compilation:

```bash
cd /mnt/e/enterprise-camp/hadoop-task
rm -rf build/*

javac \
  -classpath "$(hadoop classpath --glob)" \
  -d build \
  src/WordCount.java
```

Compiled classes included:

```text
WordCount.class
WordCount$TokenizerMapper.class
WordCount$IntSumReducer.class
```

Creating the JAR directly on `/mnt/e` failed with:

```text
java.nio.file.FileSystemException: WordCount.jar: Operation not permitted
```

The workaround was to create the JAR in the Linux filesystem:

```bash
jar -cvf /home/hadoop/WordCount.jar -C build .
```

The custom MapReduce job was run with:

```bash
hadoop jar /home/hadoop/WordCount.jar WordCount \
  /user/hadoop/hadoop-task/input \
  /user/hadoop/hadoop-task/custom-wordcount
```

Verification:

```bash
hdfs dfs -ls /user/hadoop/hadoop-task/custom-wordcount
```

Observed:

```text
_SUCCESS
part-r-00000
```

The custom result was saved locally:

```bash
hdfs dfs -cat \
  /user/hadoop/hadoop-task/custom-wordcount/part-r-00000 \
  > /mnt/e/enterprise-camp/hadoop-task/results/custom-wordcount.txt
```

Distinct cleaned tokens:

```bash
wc -l /mnt/e/enterprise-camp/hadoop-task/results/custom-wordcount.txt
```

Observed:

```text
3510
```

Comparison:

```text
Built-in WordCount distinct token forms: 6122
Custom WordCount distinct cleaned tokens: 3510
```

The decrease is explained by punctuation normalization. Tokens that differed only by punctuation are combined under the same cleaned key.

## 7. Execution-time measurement

Timing was added around `job.waitForCompletion(true)`:

```java
long startTime = System.currentTimeMillis();

boolean success = job.waitForCompletion(true);

long endTime = System.currentTimeMillis();

System.out.println("=================================");
System.out.println("Execution time: " + (endTime - startTime) + " ms");
System.out.println("Execution time: " + ((endTime - startTime) / 1000.0) + " seconds");
System.out.println("=================================");

System.exit(success ? 0 : 1);
```

### Default split behavior

Output directory:

```text
/user/hadoop/hadoop-task/timing-default
```

Important counters:

```text
Map input records=7367
Map output records=45245
Combine output records=3510
Reduce input groups=3510
Reduce output records=3510
Shuffled Maps=1
CPU time spent=6320 ms
Bytes Read=247495
Bytes Written=32394
Execution time=37.997 seconds
```

This was the baseline.

## 8. Input split experiments

The configuration used was:

```java
job.getConfiguration().setLong(
    "mapreduce.input.fileinputformat.split.maxsize",
    VALUE
);
```

### 128 KiB maximum split

Value:

```text
131072 bytes
```

Output directory:

```text
/user/hadoop/hadoop-task/timing-128kb
```

Observed counters:

```text
Map input records=7367
Map output records=45245
Combine output records=4927
Reduce input groups=3510
Reduce output records=3510
Shuffled Maps=2
Merged Map outputs=2
CPU time spent=17690 ms
Execution time=60.648 seconds
```

### 64 KiB maximum split

Value:

```text
65536 bytes
```

Output directory:

```text
/user/hadoop/hadoop-task/timing-64kb
```

Observed counters:

```text
Map input records=7367
Map output records=45245
Combine output records=6584
Reduce input groups=3510
Reduce output records=3510
Shuffled Maps=4
Merged Map outputs=4
CPU time spent=24670 ms
Execution time=64.458 seconds
```

### Split-size comparison

| Split setting | Mapper tasks | Combiner output | CPU time | Execution time |
| ------------- | -----------: | --------------: | -------: | -------------: |
| Default       |            1 |            3510 |   6.32 s |       37.997 s |
| 128 KiB       |            2 |            4927 |  17.69 s |       60.648 s |
| 64 KiB        |            4 |            6584 |  24.67 s |       64.458 s |

Observation:

- Decreasing the maximum split size increased the number of Mapper tasks.
- The final result did not change: all runs produced `3510` reducer output records.
- On this small ~242 KB text file, more Mapper tasks made execution slower.
- The extra overhead came from scheduling more tasks, running more Mapper/Combiner instances, shuffling more intermediate records, merging more map outputs, JVM/runtime overhead, and additional memory/GC work.
- Therefore, smaller splits are not automatically faster. They are more useful when the dataset is large enough that parallelism outweighs task-management overhead.

## 9. HDFS replication observation

File listing:

```bash
hdfs dfs -ls /user/hadoop/hadoop-task/input
```

Observed:

```text
-rw-r--r--   1 hadoop supergroup 247495 ... /user/hadoop/hadoop-task/input/book.txt
```

The value `1` is the file replication factor in this single-node setup.

Directory listing:

```bash
hdfs dfs -ls /user/hadoop/hadoop-task
```

Observed directories showed `-` instead of a replication factor:

```text
drwxr-xr-x   - hadoop supergroup ... builtin-wordcount
drwxr-xr-x   - hadoop supergroup ... custom-wordcount
drwxr-xr-x   - hadoop supergroup ... input
drwxr-xr-x   - hadoop supergroup ... timing-128kb
drwxr-xr-x   - hadoop supergroup ... timing-64kb
drwxr-xr-x   - hadoop supergroup ... timing-default
```

Explanation:

HDFS replication applies to file data blocks. A file is divided into blocks, and those blocks can be replicated across DataNodes. A directory is metadata managed by the NameNode and does not itself contain file data blocks, so a replication factor is not shown for directories.

## 10. Hadoop web UIs used

YARN ResourceManager:

```text
http://localhost:8088
```

Used to inspect MapReduce application status and confirm jobs finished successfully.

HDFS NameNode:

```text
http://localhost:9870
```

Used to inspect HDFS/NameNode status and DataNode health.

## Current Hadoop task status

Completed:

- Built-in WordCount execution
- HDFS input creation and upload
- Mapper input/output type identification
- Reducer input/output type identification
- Custom Java Mapper and Reducer
- Punctuation cleaning
- Java compilation
- JAR creation and execution
- Custom WordCount result
- HDFS replication-factor observation
- Execution-time measurement
- Default, 128 KiB, and 64 KiB input split experiments
- Performance comparison
