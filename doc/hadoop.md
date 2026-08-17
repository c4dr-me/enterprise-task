\# Hadoop 3.5.0 — Processing a Book with HDFS and MapReduce



This guide shows how to process a large book using Hadoop 3.5.0 on Ubuntu/WSL2.



The workflow is:



Book/PDF → Text file → HDFS → MapReduce → Results



\---



\## 1. Start Hadoop



Log in as the `hadoop` user.



Check that Hadoop is running:



```bash

jps

```



You should see:



```text

NameNode

DataNode

SecondaryNameNode

ResourceManager

NodeManager

Jps

```



If they are not running:



```bash

start-dfs.sh

start-yarn.sh

```



\---



\## 2. Prepare the book



Hadoop processes text much more easily than PDF files.



If the book is already a `.txt` file, you can use it directly.



For example:



```text

book.txt

```



If the book is a PDF, first extract its text.



For a text-based PDF, install `pdftotext`:



```bash

sudo apt install poppler-utils

```



Then:



```bash

pdftotext book.pdf book.txt

```



Check the result:



```bash

wc -l book.txt

```



```bash

head book.txt

```



If the PDF is scanned images rather than selectable text, OCR is required before Hadoop can process the content.



\---



\# 3. Create a local Hadoop project directory



```bash

mkdir -p \~/book-project/input

mkdir -p \~/book-project/output

```



Copy your book into it:



```bash

cp book.txt \~/book-project/input/

```



Check:



```bash

ls -lh \~/book-project/input/

```



\---



\# 4. Create an HDFS directory for the book



Create the HDFS user directory if necessary:



```bash

hdfs dfs -mkdir -p /user/hadoop

```



Create the book input directory:



```bash

hdfs dfs -mkdir -p /user/hadoop/book/input

```



\---



\# 5. Upload the book to HDFS



```bash

hdfs dfs -put \~/book-project/input/book.txt /user/hadoop/book/input/

```



Check:



```bash

hdfs dfs -ls /user/hadoop/book/input

```



You should see:



```text

book.txt

```



\---



\# 6. Check the book inside HDFS



Display the beginning:



```bash

hdfs dfs -cat /user/hadoop/book/input/book.txt | head

```



Check the size:



```bash

hdfs dfs -du -h /user/hadoop/book/input/book.txt

```



\---



\# 7. Run WordCount



Hadoop 3.5.0 includes a MapReduce examples JAR.



Run:



```bash

hadoop jar $HADOOP\_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.5.0.jar \\

wordcount \\

/user/hadoop/book/input \\

/user/hadoop/book/wordcount

```



Important:



The output directory must NOT already exist.



If you get:



```text

FileAlreadyExistsException

```



remove the old output:



```bash

hdfs dfs -rm -r /user/hadoop/book/wordcount

```



Then run the job again.



\---



\# 8. View the WordCount results



List the output:



```bash

hdfs dfs -ls /user/hadoop/book/wordcount

```



Display the result:



```bash

hdfs dfs -cat /user/hadoop/book/wordcount/part-r-00000 | head -50

```



The output looks approximately like:



```text

a       15234

about   321

after   812

again   245

all     1943

also    734

and     23891

...

```



The second column is the number of times each word appeared.



\---



\# 9. Find the most common words



The Hadoop output is alphabetically sorted.



To find the most frequent words:



```bash

hdfs dfs -cat /user/hadoop/book/wordcount/part-r-00000 \\

| sort -k2,2nr \\

| head -50

```



This gives the top 50 most frequently occurring words.



Example:



```text

the     45231

and     23891

of      22104

to      19872

...

```



\---



\# 10. Remove common English words



Normal English words such as:



```text

the

and

of

to

a

in

```



usually dominate WordCount.



Create a stop-word list:



```bash

nano \~/book-project/stopwords.txt

```



Example:



```text

a

an

and

are

as

at

be

by

for

from

in

is

it

of

on

or

that

the

this

to

was

were

with

```



This can later be used in a custom MapReduce program to produce more meaningful statistics.



\---



\# 11. Search the book using Hadoop



You can use Hadoop's `grep` MapReduce example.



For example, search for the word:



```text

Hadoop

```



Run:



```bash

hdfs dfs -rm -r /user/hadoop/book/search-hadoop 2>/dev/null

```



Then:



```bash

hadoop jar $HADOOP\_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.5.0.jar \\

grep \\

/user/hadoop/book/input \\

/user/hadoop/book/search-hadoop \\

'Hadoop'

```



View the result:



```bash

hdfs dfs -cat /user/hadoop/book/search-hadoop/\*

```



\---



\# 12. Count occurrences of a particular word



For example, search for:



```text

computer

```



You can first run:



```bash

hdfs dfs -cat /user/hadoop/book/wordcount/part-r-00000 \\

| grep -i '^computer\[\[:space:]]'

```



You might get:



```text

computer        137

```



This means the word occurred 137 times.



\---



\# 13. Count lines in the book



Check the local file:



```bash

wc -l \~/book-project/input/book.txt

```



Or directly from HDFS:



```bash

hdfs dfs -cat /user/hadoop/book/input/book.txt | wc -l

```



\---



\# 14. Count characters



```bash

hdfs dfs -cat /user/hadoop/book/input/book.txt | wc -m

```



\---



\# 15. Count words



```bash

hdfs dfs -cat /user/hadoop/book/input/book.txt | wc -w

```



This is useful for comparing the normal Linux result against your Hadoop WordCount result.



\---



\# 16. Store the final results locally



Create a results directory:



```bash

mkdir -p \~/book-project/results

```



Copy Hadoop's WordCount result from HDFS:



```bash

hdfs dfs -get /user/hadoop/book/wordcount \\

\~/book-project/results/

```



Check:



```bash

ls -lh \~/book-project/results/wordcount/

```



\---



\# 17. Download the complete result as one file



You can merge Hadoop's output files into one local file:



```bash

hdfs dfs -getmerge \\

/user/hadoop/book/wordcount \\

\~/book-project/results/wordcount.txt

```



Now:



```bash

head -50 \~/book-project/results/wordcount.txt

```



\---



\# 18. Clean up a previous MapReduce job



If you want to run WordCount again, delete its old output:



```bash

hdfs dfs -rm -r /user/hadoop/book/wordcount

```



Then run:



```bash

hadoop jar $HADOOP\_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.5.0.jar \\

wordcount \\

/user/hadoop/book/input \\

/user/hadoop/book/wordcount

```



\---



\# 19. Check Hadoop's job status



While a MapReduce job is running, Hadoop will show information about:



\* map tasks

\* reduce tasks

\* input data

\* output data

\* job progress



You can also open the YARN ResourceManager in your Windows browser:



```text

http://localhost:8088

```



The HDFS NameNode interface is:



```text

http://localhost:9870

```



\---



\# 20. Useful HDFS commands



List files:



```bash

hdfs dfs -ls /user/hadoop/book

```



List recursively:



```bash

hdfs dfs -ls -R /user/hadoop/book

```



Create directory:



```bash

hdfs dfs -mkdir -p /user/hadoop/book/test

```



Copy local → HDFS:



```bash

hdfs dfs -put file.txt /user/hadoop/book/

```



Copy HDFS → local:



```bash

hdfs dfs -get /user/hadoop/book/file.txt .

```



Read a file:



```bash

hdfs dfs -cat /user/hadoop/book/file.txt

```



Delete a file:



```bash

hdfs dfs -rm /user/hadoop/book/file.txt

```



Delete a directory:



```bash

hdfs dfs -rm -r /user/hadoop/book/test

```



Check HDFS disk usage:



```bash

hdfs dfs -du -h /user/hadoop/book

```



\---



\# 21. Complete workflow



For a new book, the basic workflow is:



```bash

\# Start Hadoop

start-dfs.sh

start-yarn.sh



\# Create local project

mkdir -p \~/book-project/input

mkdir -p \~/book-project/results



\# Put book.txt into \~/book-project/input/



\# Create HDFS directory

hdfs dfs -mkdir -p /user/hadoop/book/input



\# Upload book

hdfs dfs -put \~/book-project/input/book.txt \\

/user/hadoop/book/input/



\# Run WordCount

hadoop jar \\

$HADOOP\_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.5.0.jar \\

wordcount \\

/user/hadoop/book/input \\

/user/hadoop/book/wordcount



\# Show most common words

hdfs dfs -cat \\

/user/hadoop/book/wordcount/part-r-00000 \\

| sort -k2,2nr \\

| head -50



\# Save results locally

hdfs dfs -getmerge \\

/user/hadoop/book/wordcount \\

\~/book-project/results/wordcount.txt

```



\---



\# 22. Suggested Hadoop exercises



Once the basic WordCount job works, progressively try these tasks:



\### Exercise 1 — Word frequency



Find the 20 most common words.



```bash

hdfs dfs -cat /user/hadoop/book/wordcount/part-r-00000 \\

| sort -k2,2nr \\

| head -20

```



\### Exercise 2 — Search



Find all occurrences of a particular word or phrase.



\### Exercise 3 — Character statistics



Calculate the number of characters in the book.



\### Exercise 4 — Sentence statistics



Calculate the average number of words per sentence.



\### Exercise 5 — Chapter statistics



Split the book into chapters and calculate the word count of every chapter.



\### Exercise 6 — Character names



For a novel, create a list of character names and count how frequently each character appears.



\### Exercise 7 — Stop-word removal



Remove common English words and calculate the most meaningful words.



\### Exercise 8 — Custom MapReduce



Write your own Java MapReduce program that:



```text

Input

&#x20; ↓

Mapper

&#x20; ↓

Shuffle / Sort

&#x20; ↓

Reducer

&#x20; ↓

HDFS output

```



For example:



```text

book.txt

&#x20;  ↓

Mapper

&#x20;  ↓

(character, 1)

&#x20;  ↓

Shuffle

&#x20;  ↓

(character, \[1,1,1,1,...])

&#x20;  ↓

Reducer

&#x20;  ↓

(character, total)

```



\---



\# 23. Important note about a 200-page book



A 200-page book is actually \*\*small for a real Hadoop cluster\*\*. Your single WSL machine can process it easily without Hadoop.



The value of this exercise is learning the Hadoop workflow:



```text

Raw data

&#x20;  ↓

HDFS

&#x20;  ↓

Mapper

&#x20;  ↓

Shuffle

&#x20;  ↓

Reducer

&#x20;  ↓

HDFS results

```



For learning Hadoop, however, a 200-page book is a perfectly good dataset.



For larger datasets, you can use:



\* several books

\* thousands of books

\* newspaper archives

\* Wikipedia dumps

\* log files

\* CSV datasets

\* large collections of text documents



The commands in this guide can be reused for all of them.



\---



\# 24. Stop Hadoop when finished



When you're done working:



```bash

stop-yarn.sh

stop-dfs.sh

```



Check:



```bash

jps

```



You should no longer see the Hadoop daemons.



