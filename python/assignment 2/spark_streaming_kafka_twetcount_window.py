# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

SPARK_APP_NAME = "KafkaWordCount"
SPARK_CHECKPOINT_TMP_DIR = "tmp_spark_streaming"
SPARK_BATCH_INTERVAL = 5
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


# Подписываемся на поток Kafka
kafka_stream = KafkaUtils.createDirectStream(ssc,
                                             topics=[KAFKA_TOPIC],
                                             kafkaParams={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

# Трансформируем мини-batch
lines = kafka_stream.map(lambda x: x[1])

# Подсчитывем количество постов для мини-batch
counts = lines.map(lambda screen_name: (screen_name, 1)).reduceByKey(lambda x1, x2: x1 + x2)

# Подсчитываем количество постов в минуту
minute = counts.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 60, 30)

# Сортируем текущий результат
minute_sorted = minute.transform(lambda rdd: rdd.sortByKey(ascending = False))

# Сохраняем текущий результат
minute_sorted.saveAsTextFiles("output/output-minute")

# Подсчитываем количество постов в 10 минут
tenmin_sorted = minute_sorted.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 600, 30)

# Сохраняем текущий результат
tenmin_sorted.saveAsTextFiles("output/output-tenmin")

# Выводим текущий результат
tenmin_sorted.pprint()

# Запускаем Spark Streaming
ssc.start()

# Ожидаем остановку
ssc.awaitTermination()
