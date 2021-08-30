#!/bin/bash

"$SPARK_HOME"/bin/spark-submit \
  --class "info.graphsense.TransformationJob" \
  --master "local[*]" \
  --conf spark.executor.memory=8g \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.cassandra.connection.host="localhost" \
  --conf spark.driver.host="localhost" \
  --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,org.rogach:scallop_2.12:4.0.2,joda-time:joda-time:2.10.10 \
  target/scala-2.12/graphsense-ethereum-transformation_2.12-0.5.1-SNAPSHOT.jar \
  --raw-keyspace "eth_raw" \
  --tag-keyspace "tagpacks" \
  --target-keyspace "eth_transformed" \
  --bucket-size 2

exit $?
