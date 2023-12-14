package org.graphsense.storage

import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.writer.RowWriterFactory
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.graphsense.Util._
import scala.reflect.ClassTag
import com.datastax.spark.connector.cql.CassandraConnector

class CassandraStorage(spark: SparkSession) {

  import spark.implicits._
  import com.datastax.spark.connector._

  def session(): SparkSession = {
    spark
  }

  def isTableEmpty(keyspace: String, tableName: String): Boolean = {
    CassandraConnector(spark.sparkContext).withSessionDo { session =>
      val test =
        session.execute(f"select * from ${keyspace}.${tableName} limit 1;")
      test.one() == null
    }
  }

  def load[T <: Product: ClassTag: RowReaderFactory: ValidRDDType: Encoder](
      keyspace: String,
      tableName: String,
      columns: ColumnRef*
  ) = {
    spark.sparkContext.setJobDescription(s"Loading table ${tableName}")
    val table = spark.sparkContext.cassandraTable[T](keyspace, tableName)
    if (columns.isEmpty)
      table.toDS().as[T]
    else
      table.select(columns: _*).toDS().as[T]
  }

  def store[T <: Product: RowWriterFactory](
      keyspace: String,
      tableName: String,
      df: Dataset[T]
  ) = {
    val isEmpty = isTableEmpty(keyspace, tableName)
    spark.sparkContext.setJobDescription(
      s"Writing table ${tableName} - empty: ${isEmpty}"
    )
    time(s"Writing table ${tableName} - empty: ${isEmpty}") {
      df.rdd.saveToCassandra(keyspace, tableName)
    }
  }
}
