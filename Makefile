NETWORK       ?= hadoop
ENV_FILE      ?= ./hadoop.env
CLUSTER       ?= podman-carried
DATA_DIR      ?= ./datasets
OUTPUT_DIR    ?= ./output
NOTEBOOKS_DIR ?= ./python
OUTPUT        ?= avg_output

hdfs_images    := namenode datanode
hadoop_images  := resourcemanager nodemanager historyserver
reviews        := reviews_Electronics_5.json
meta           := meta_Electronics.json
mapreducers    := avarage prodname find
spark_images   := spark-master spark-worker
all_images     := $(hadoop_images) $(hdfs_images) $(spark_images) kafka zookeeper


########################################
# Network operations
########################################

network/create:
	podman network create $(NETWORK)

network/rm:
	podman network rm $(NETWORK)

network/dns:
	resolvectl dns $(shell podman network inspect $(NETWORK) -f '{{(index  .plugins  0).bridge}}') $(shell podman network inspect $(NETWORK) -f '{{(index (index (index  .plugins  0).ipam.ranges 0) 0).gateway}}')
	resolvectl domain $(shell podman network inspect $(NETWORK) -f '{{(index  .plugins  0).bridge}}') $(shell podman network inspect $(NETWORK) -f '{{(index  .plugins  3).domainName}}')


########################################
# Cluster operations
########################################

hdfs/up: $(hdfs_images:%=%/run)

hdfs/down: $(hdfs_images:%=%/rm)

hadoop/up: $(hadoop_images:%=%/run)

hadoop/down: $(hadoop_images:%=%/rm)

spark/up: $(spark_images:%=%/run)

spark/down: $(spark_images:%=%/rm)


########################################
# Hadoop and HDFS operations
########################################

$(hadoop_images:%=%/run):
	podman run -dt --network $(NETWORK) -e CLUSTER_NAME=$(CLUSTER) --env-file $(ENV_FILE) --name $(@:/run=) --hostname $(@:/run=) bde2020/hadoop-$(@:/run=):2.0.0-hadoop3.2.1-java8

namenode/run:
	podman run -dt --network $(NETWORK) -e CLUSTER_NAME=$(CLUSTER) --env-file $(ENV_FILE) --volume $(@:/run=):/hadoop/dfs/name --name $(@:/run=) --hostname $(@:/run=) bde2020/hadoop-$(@:/run=):2.0.0-hadoop3.2.1-java8

datanode/run:
	podman run -dt --network $(NETWORK) -e CLUSTER_NAME=$(CLUSTER) --env-file $(ENV_FILE) --volume $(@:/run=):/hadoop/dfs/data --name $(@:/run=) --hostname $(@:/run=) bde2020/hadoop-$(@:/run=):2.0.0-hadoop3.2.1-java8


########################################
# HDFS Volume operations
########################################

$(hdfs_images:%=%/volume/create):
	podman volume create $(@:/volume/create=)

$(hdfs_images:%=%/volume/rm):
	podman volume rm $(@:/volume/rm=)


########################################
# Spark operations
########################################

spark-master/run:
	podman run -dt --network $(NETWORK) -e ENABLE_INIT_DAEMON=false --name $(@:/run=) --hostname $(@:/run=) bde2020/$(@:/run=):3.0.0-hadoop3.2

spark-worker/run:
	podman run -dt --network $(NETWORK) -e ENABLE_INIT_DAEMON=false -e SPARK_MASTER=spark://spark-master:7077 --name $(@:/run=) --hostname $(@:/run=) localhost/$(@:/run=):3.0.0-python3.7

spark-worker/bash:
	podman run --rm -it --entrypoint bash --network $(NETWORK) -e ENABLE_INIT_DAEMON=false -e SPARK_MASTER=spark://spark-master:7077 --name $(@:/bash=) --hostname $(@:/bash=) localhost/$(@:/bash=):3.0.0-python3.7

jupyter/run:
	podman run -it --rm --mount type=bind,src=$(NOTEBOOKS_DIR),target=/opt/notebooks,ro=false,relabel=shared --network $(NETWORK) --name $(@:/run=) --hostname $(@:/run=) $(@:/run=):3.0.0

spark-base/run:
	podman run -dt --rm --network hadoop --entrypoint bash bde2020/spark-base:2.4.5-hadoop2.7


########################################
# Kafka operations
########################################

zookeeper/run:
	podman run -dt --network $(NETWORK) --name zookeeper --hostname zookeeper wurstmeister/zookeeper:3.4.6

kafka/run:
	podman run --rm --network $(NETWORK) -e KAFKA_ZOOKEEPER_CONNECT="zookeeper:2181" -e KAFKA_ADVERTISED_HOST_NAME=kafka -e KAFKA_CREATE_TOPICS="tweets-kafka:1:1" --name kafka --hostname kafka wurstmeister/kafka:2.12-2.5.0


########################################
# Rm operations
########################################

$(all_images:%=%/rm):
	podman rm -f $(@:/rm=)


########################################
# Applications operations
########################################

avarage/run:
	podman run --rm --network $(NETWORK) --env-file $(ENV_FILE) --mount type=bind,src=./apps/AverageRating/build/libs,target=/mnt,ro=false,relabel=shared -w /mnt bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8 yarn jar AverageRating-1.0.jar /input_avarage/$(reviews) /output_avarage

prodname/run:
	podman run --rm --network $(NETWORK) --env-file $(ENV_FILE) --mount type=bind,src=./apps/ProduvctName/build/libs,target=/mnt,ro=false,relabel=shared -w /mnt bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8 yarn jar ProductName-1.0.jar /input_prodname/$(meta) /output_avarage/part-r-00000 /output_prodname

find/run:
	podman run --rm --network $(NETWORK) --env-file $(ENV_FILE) --mount type=bind,src=./apps/AvgByName/build/libs,target=/mnt,ro=false,relabel=shared -w /mnt bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8 yarn jar AvgByName-1.0.jar /output_prodname/part-r-00000 /output_find $(CONTAINED)


########################################
# Output operations
########################################

$(OUTPUT:%=%/output):
	podman run --rm --network $(NETWORK) --env-file $(ENV_FILE) --mount type=bind,src=./$(OUTPUT_DIR),target=/mnt,ro=false,relabel=shared -w /mnt bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8 hdfs dfs -copyToLocal /output_$(@:/output=) .
