# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

SPARK_APP_NAME = "KafkaWordCount"
SPARK_CHECKPOINT_TMP_DIR = "tmp_spark_streaming"
SPARK_BATCH_INTERVAL = 60
SPARK_LOG_LEVEL = "OFF"

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "tweets-kafka"

# Create Spark Context
sc = SparkContext(appName=SPARK_APP_NAME)

# Set log level
sc.setLogLevel(SPARK_LOG_LEVEL)

# Create Streaming Context
ssc = StreamingContext(sc, SPARK_BATCH_INTERVAL)

# Sets the context to periodically checkpoint the DStream operations for master
# fault-tolerance. The graph will be checkpointed every batch interval.
# It is used to update results of stateful transformations as well
ssc.checkpoint(SPARK_CHECKPOINT_TMP_DIR)


# Create subscriber (consumer) to the Kafka topic
kafka_stream = KafkaUtils.createDirectStream(ssc,
                                             topics=[KAFKA_TOPIC],
                                             kafkaParams={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

# Трансформируем мини-batch
lines = kafka_stream.map(lambda x: x[1])

# Подсчитывем количество постов для мини-batch
counts = lines.map(lambda key: (key, 1)).reduceByKey(lambda x1, x2: x1 + x2)

# Подсчитываем количество постов в минуту
counts_with_state = counts.updateStateByKey(lambda x, y: sum(x, y) if y is not None else sum(x, 0))

# Сортируем текущий результат
counts_with_state_sorted = counts_with_state.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending = False))

# Сохраняем только ключ
only_keys_counts = counts_with_state_sorted.map(lambda x: (x[0].split("\t")[0], x[1]))

# Сохраняем текущий результат
only_keys_counts.saveAsTextFiles("output/o")

# Выводим 5 наиболее частых твита
top_five_tweets = counts_with_state_sorted.map(lambda x: x[0]).pprint(5)

# Запускаем Spark Streaming
ssc.start()

# Ожидаем остановку
ssc.awaitTerminationOrTimeout(1800)
