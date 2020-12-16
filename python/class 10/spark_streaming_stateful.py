# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


# Create Spark Context
sc = SparkContext(appName="WordCountStateful")

# Set log level
#sc.setLogLevel("INFO")

# Batch interval (10 seconds)
batch_interval = 10

# Create Streaming Context
ssc = StreamingContext(sc, batch_interval)

# Add checkpoint to preserve the states
ssc.checkpoint("tmp_spark_streaming")  # == /user/<YOUR_USER>/tmp_spark_streaming

# Create a stream
lines = ssc.socketTextStream("localhost", 9999)

# TRANSFORMATION FOR EACH BATCH
words = lines.flatMap(lambda line: line.split())
word_tuples = words.map(lambda word: (word, 1))
counts = word_tuples.reduceByKey(lambda x1, x2: x1 + x2)


# function for updating values
def update_total_count(currentCount, countState):
    if countState is None:
        countState = 0
    return sum(currentCount, countState)


# Update current values
total_counts = counts.updateStateByKey(update_total_count)

# Print the result (10 records)
counts.pprint()
#counts.transform(lambda rdd: rdd.coalesce(1)).saveAsTextFiles("/YOUR_PATH/output/wordCount")

# Start Spark Streaming
ssc.start()

# Await termination
ssc.awaitTermination()
