#!/bin/bash

FOO="${SPARK_MASTER:=local[*]}"
FOO="${SPARK_DRIVER_HOST:=localhost}"
FOO="${SPARK_DRIVER_PORT:=0}"
FOO="${SPARK_LOCAL_DIR:=}"
FOO="${SPARK_UI_PORT:=8088}"
FOO="${SPARK_BLOCKMGR_PORT:=0}"
FOO="${SPARK_PARALLELISM:=16}"
FOO="${SPARK_EXECUTOR_MEMORY:=4g}"
FOO="${SPARK_DRIVER_MEMORY:=4g}"

FOO="${TRANSFORM_VERSION:=1.5.1}"
FOO="${TRANSFORM_BUCKET_SIZE:=10000}"
FOO="${NETWORK:=ETH}"

FOO="${SPARK_PACKAGES:=com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,org.rogach:scallop_2.12:4.1.0,joda-time:joda-time:2.10.10,org.web3j:core:4.8.7,org.web3j:abi:4.8.7 \
  graphframes:graphframes:0.8.2-spark3.2-s_2.12, org.apache.spark:spark-graphx:3.2.4 \
  target/scala-2.12/graphsense-spark_2.12-$TRANSFORM_VERSION.jar}"

FOO="${CASSANDRA_HOST:=localhost}"

echo -en "Starting Spark job ...\n" \
         "Config:\n" \
         "- Spark master:        $SPARK_MASTER\n" \
         "- Spark driver:        $SPARK_DRIVER_HOST:$SPARK_DRIVER_PORT\n" \
         "- Spark local dir:     $SPARK_LOCAL_DIR\n" \
         "- Cassandra host:      $CASSANDRA_HOST\n" \
         "- Executor memory:     $SPARK_EXECUTOR_MEMORY\n" \
         "- Spark parallelism:   $SPARK_PARALLELISM\n" \
         "- Transform Version:   $TRANSFORM_VERSION\n" \
         "Arguments:\n" \
         "- Raw keyspace:        $RAW_KEYSPACE\n" \
         "- Target keyspace:     $TGT_KEYSPACE\n" \
         "- Bucket Size:         $TRANSFORM_BUCKET_SIZE\n"

"$SPARK_HOME"/bin/spark-submit \
  --class "org.graphsense.TransformationJob" \
  --master "$SPARK_MASTER" \
  --conf spark.driver.bindAddress="0.0.0.0" \
  --conf spark.driver.host="$SPARK_DRIVER_HOST" \
  --conf spark.driver.port="$SPARK_DRIVER_PORT" \
  --conf spark.ui.port="$SPARK_UI_PORT" \
  --conf spark.blockManager.port="$SPARK_BLOCKMGR_PORT" \
  --conf spark.executor.memory="$SPARK_EXECUTOR_MEMORY" \
  --conf spark.cassandra.connection.host="$CASSANDRA_HOST" \
  --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions \
  --conf spark.local.dir="$SPARK_LOCAL_DIR" \
  --conf spark.default.parallelism=$SPARK_PARALLELISM \
  --conf spark.driver.memory=$SPARK_DRIVER_MEMORY \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.serializer="org.apache.spark.serializer.KryoSerializer" \
  --packages $SPARK_PACKAGES \
  --network "$NETWORK" \
  --raw-keyspace "$RAW_KEYSPACE" \
  --target-keyspace "$TGT_KEYSPACE" \
  --bucket-size $TRANSFORM_BUCKET_SIZE

exit $?
