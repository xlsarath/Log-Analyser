# Log-Analyser
Map Reduce using Spark

Spark application that will process a web server’s access log to count the number of times ‘.jpg’,’.gif’ and other resources are requested. Your program will report three numbers:

Number of ‘.gif’
Number of ‘.jpg’
Number of other requests like ‘.php’


Installation:

1.You may use Linux or Windows environment to install Apache Spark(I have used Ubuntu 16.04 LTS).(Tip: Spin a VM for this on any Cloud platform-although not required).

2.Install Java.

3.Install Scala.

4.Install Spark.

5.Install Python.

You may refer to this article:

https://medium.com/@josemarcialportilla/installing-scala-and-spark-on-ubuntu-5665ee4b62b1 (Links to an external site.)Links to an external site.

 

How to Run Spark Jobs:

1.Navigate inside bin folder of installed Spark.

2.Keep your data and Python program file in the folder.

3.Run the program file using below command:

 

$SPARK_HOME/bin/spark-submit     path/to/myapp.py       path/to/pg1661.txt

 

Example Code (MapReduce): Word Count Application

Import libraries

from __future__ import print_function

import sys

from operator import add

from pyspark.sql import SparkSession

 

if __name__ == "__main__":

    if len(sys.argv) != 2:

        print("Usage: wordcount <file>", file=sys.stderr)

        sys.exit(-1)

 

Create Spark Session

The first step of every such Spark application is to create a Spark context.

spark = SparkSession\

        .builder\

        .appName("PythonWordCount")\

        .getOrCreate()

 

Read file and return RDD

Next, you'll need to read the target file into an RDD.

 

lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

 

Using Map-Reduce functions to count words in the text

You now have an RDD filled with strings, one per line of the file. Next you'll want to split the lines into individual words. The flatMap() operation first converts each line into an array of words, and then makes each of the words an element in the new RDD. If you asked Spark to count the number of elements in the words RDD, it would tell you the number of words in the file.

Next, you'll want to replace each word with a tuple of that word and the number 1. The reason will become clear shortly. The map() operation replaces each word with a tuple of that word and the number 1. The pairs RDD is a pair RDD where the word is the key, and all of the values are the number 1.

Now, to get a count of the number of instances of each word, you need only group the elements of the RDD by key (word) and add up their values. The reduceByKey() operation keeps adding elements' values together until there are no more to add for each key (word)

counts = lines.flatMap(lambda x: x.split(' ')) \

                  .map(lambda x: (x, 1)) \

                  .reduceByKey(add)

 output = counts.collect()

for (word, count) in output:

        print("%s: %i" % (word, count))

 

spark.stop()


execution: 
 spark-submit /Users/sarathchandra/Desktop/reference-apps/logs_analyzer/chapter1/python/databricks/apps/logs/log_analyzer.py /Users/sarathchandra/Desktop/reference-apps/logs_analyzer/data/access_log --executor-cores 4 --num-executors 20 &> output_sar.txt

 

Source:

https://github.com/apache/spark/blob/master/examples/src/main/python/wordcount.py (Links to an external site.)Links to an external site.
CMPE 256-02 Large Scale Analytics Spring 2019 - Dr. G. Guzun's, Earinaki tutorial
