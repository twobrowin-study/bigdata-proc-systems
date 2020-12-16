# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

SPARK_APP_NAME = "KafkaWordCount"
SPARK_CHECKPOINT_TMP_DIR = "tmp_spark_streaming"
SPARK_BATCH_INTERVAL = 10
SPARK_LOG_LEVEL = "OFF"

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "tweets-kafka"


# Функция для обновления значений количества слов
def updateTotalCount(currentCount, countState):
    if countState is None:
        countState = 0
    return sum(currentCount, countState)


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

# Подсчитывем количество слов для мини-batch
counts = lines.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda x1, x2: x1 + x2)

# Обновляем значения количества слов с учетом нового мини-batch
total_counts = counts.updateStateByKey(updateTotalCount)

# Выводим текущий результат
total_counts.pprint()

# Запускаем Spark Streaming
ssc.start()

# Ожидаем остановку
ssc.awaitTermination()
