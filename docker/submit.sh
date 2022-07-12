#!/bin/bash

echo -en "Starting Spark job ...\n" \
         "Config:\n" \
         "- Spark master:        $SPARK_MASTER\n" \
         "- Spark driver:        $SPARK_DRIVER_HOST\n" \
         "- Spark local dir:     $SPARK_LOCAL_DIR\n" \
         "- Cassandra host:      $CASSANDRA_HOST\n" \
         "- Executor memory:     $SPARK_EXECUTOR_MEMORY\n" \
         "Arguments:\n" \
         "- Raw keyspace:        $RAW_KEYSPACE\n" \
         "- Target keyspace:     $TGT_KEYSPACE\n"

"$SPARK_HOME"/bin/spark-submit \
  --class "info.graphsense.TransformationJob" \
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
  --conf spark.default.parallelism=400 \
  --conf spark.driver.memory="64G" \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.serializer="org.apache.spark.serializer.KryoSerializer" \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,org.rogach:scallop_2.12:4.1.0,joda-time:joda-time:2.10.10 \
  target/scala-2.12/graphsense-ethereum-transformation_2.12-1.1.0-SNAPSHOT.jar \
  --raw-keyspace "$RAW_KEYSPACE" \
  --target-keyspace "$TGT_KEYSPACE" \
  --bucket-size 10000

exit $?
