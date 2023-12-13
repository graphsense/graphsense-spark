package org.graphsense

import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{
  array,
  coalesce,
  col,
  floor,
  hex,
  lit,
  map_keys,
  max,
  row_number,
  struct,
  substring,
  sum,
  typedLit
}
import org.apache.spark.sql.types.{DataType, FloatType, IntegerType}
import org.graphsense.models.ExchangeRatesRaw
import org.apache.spark.sql.AnalysisException
import org.graphsense.Util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object TransformHelpers {

  def toDSEager[
      R: Encoder
  ](ds: => DataFrame): Dataset[R] = {
    // https://stackoverflow.com/questions/70049444/spark-dataframe-as-function-does-not-drop-columns-not-present-in-matched-case
    ds.as[R].map(identity)
  }

  def namedCache[T](
      name: String,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER
  )(df: Dataset[T]): Dataset[T] = {
    df.sparkSession.sharedState.cacheManager
      .cacheQuery(df, Some(name), storageLevel)
    df
  }

  def computeCached[
      R: Encoder
  ](base_path: Option[String], spark: SparkSession)(
      dataset_name: String
  )(block: => Dataset[R]): Dataset[R] = {
    base_path match {
      case Some(path) => {
        val path_complete = path + "/" + dataset_name
        try {
          val df_loaded =
            time(f"Try Reading cached dataset ${path_complete} from parquet") {
              spark.read.parquet(path_complete)
            }
          return df_loaded.as[R]
        } catch {
          case e: AnalysisException => {
            println(
              f"Warn - Could not load cached dataset ${path_complete}: " + e
            )
            val df =
              namedCache(dataset_name)(time(f"Computing ${path_complete}") {
                block
              })

            time(f"Writing cache dataset at ${path_complete} as parquet") {
              df.write.mode("overwrite").parquet(path_complete)
            }

            return df
          }
        }
      }
      case None => {
        val df = namedCache(dataset_name)(
          time(f"Computing ${dataset_name} (gs-cache-dir not set)") {
            block
          }
        )
        df
      }
    }
  }

  def filterBlockRange[T](
      start: Option[Int],
      end: Option[Int],
      blockIdCol: String = "blockId"
  )(ds: Dataset[T]): Dataset[T] = {
    (start.getOrElse(0), end) match {
      case (minBlock, Some(maxBlock)) =>
        ds.filter(
          col(blockIdCol) >= minBlock && col(blockIdCol) <= maxBlock
        )
      case (minBlock, None) =>
        ds.filter(
          col(blockIdCol) >= minBlock
        )
    }

  }

  def getFiatCurrencies(
      exchangeRatesRaw: Dataset[ExchangeRatesRaw]
  ): Seq[String] = {
    val currencies =
      exchangeRatesRaw.select(map_keys(col("fiatValues"))).distinct
    if (currencies.count() > 1L)
      throw new Exception("Non-unique map keys in raw exchange rates table")
    else if (currencies.count() == 0L)
      throw new Exception(
        "No fiat currencies found. Exchange rates table might be empty."
      )
    currencies.rdd.map(r => r(0).asInstanceOf[Seq[String]]).collect()(0)
  }

  def zeroValueIfNull(
      columnName: String,
      length: Int,
      castValueTo: DataType = IntegerType
  )(
      df: DataFrame
  ): DataFrame = {
    df.withColumn(
      columnName,
      coalesce(
        col(columnName),
        struct(
          lit(0).cast(castValueTo).as("value"),
          typedLit(Array.fill[Float](length)(0))
            .as("fiatValues")
        )
      )
    )
  }

  def aggregateValues(
      valueColumn: String,
      fiatValueColumn: String,
      length: Int,
      groupColumns: String*
  )(df: DataFrame): DataFrame = {
    df.groupBy(groupColumns.head, groupColumns.tail: _*)
      .agg(
        createAggCurrencyStruct(valueColumn, fiatValueColumn, length)
      )
  }

  def createAggCurrencyStruct(
      valueColumn: String,
      fiatValueColumn: String,
      length: Int
  ): Column = {
    struct(
      sum(col(valueColumn)).as(valueColumn),
      array(
        (0 until length)
          .map(i => sum(col(fiatValueColumn).getItem(i)).cast(FloatType)): _*
      ).as(fiatValueColumn)
    ).as(valueColumn)
  }

  def createAggCurrencyStructPerCurrency(
      valueColumn: String,
      fiatValueColumn: String,
      length: Int
  ): Column = {
    struct(
      col("currency"),
      createAggCurrencyStruct(valueColumn, fiatValueColumn, length)
    )
  }

  def withIdGroup[T](
      idColumn: String,
      idGroupColumn: String,
      size: Int
  )(ds: Dataset[T]): DataFrame = {
    ds.withColumn(idGroupColumn, floor(col(idColumn) / size).cast("int"))
  }

  def withSortedIdGroup[T: Encoder](
      idColumn: String,
      idGroupColumn: String,
      size: Int
  )(df: DataFrame): Dataset[T] = {
    df.transform(withIdGroup(idColumn, idGroupColumn, size))
      .as[T]
      .sort(idGroupColumn)
  }

  def withPrefix[T](
      hashColumn: String,
      hashPrefixColumn: String,
      length: Int = 4
  )(ds: Dataset[T]): DataFrame = {
    ds.withColumn(hashPrefixColumn, substring(hex(col(hashColumn)), 0, length))
  }

  def withSortedPrefix[T: Encoder](
      hashColumn: String,
      prefixColumn: String,
      length: Int = 4
  )(df: DataFrame): Dataset[T] = {
    df.transform(withPrefix(hashColumn, prefixColumn, length))
      .as[T]
      .sort(prefixColumn)
  }

  def withTxReference[T](ds: Dataset[T]): DataFrame = {
    ds.withColumn(
      "txReference",
      struct(
        col("traceIndex"),
        col("logIndex")
      )
    )
  }

  def withSecondaryIdGroup[T](
      idColumn: String,
      secondaryIdColumn: String,
      windowOrderColumn: String,
      skewedPartitionFactor: Float = 2.5f
  )(ds: Dataset[T]): DataFrame = {
    val partitionSize =
      ds.select(col(idColumn)).groupBy(idColumn).count().persist()
    val noPartitions = partitionSize.count()
    val approxMedian = partitionSize
      .sort(col("count").asc)
      .select(col("count"))
      .rdd
      .zipWithIndex
      .filter(_._2 == noPartitions / 2)
      .map(_._1)
      .first()
      .getLong(0)
    val window = Window.partitionBy(idColumn).orderBy(windowOrderColumn)
    ds.withColumn(
      secondaryIdColumn,
      floor(
        row_number().over(window) / (approxMedian * skewedPartitionFactor)
      ).cast(IntegerType)
    )
  }

  def withSecondaryIdGroupApprox[T](
      idColumn: String,
      secondaryIdColumn: String,
      windowOrderColumn: String,
      skewedPartitionFactor: Float = 2.5f
  )(ds: Dataset[T]): DataFrame = {
    val approxMedian =
      ds.select(col(idColumn))
        .groupBy(idColumn)
        .count()
        .stat
        .approxQuantile("count", Array(0.5), 0.1)(0)
    val window = Window.partitionBy(idColumn).orderBy(windowOrderColumn)
    ds.withColumn(
      secondaryIdColumn,
      floor(
        row_number().over(window) / (approxMedian * skewedPartitionFactor)
      ).cast(IntegerType)
    )
  }

  def withSecondaryIdGroupSimple[T](
      idColumn: String,
      secondaryIdColumn: String,
      windowOrderColumn: String,
      skewedPartitionFactor: Float = 2.5f
  )(ds: Dataset[T]): DataFrame = {
    // val window = Window.partitionBy(idColumn).orderBy(windowOrderColumn)
    ds.withColumn(
      secondaryIdColumn,
      floor(
        col(windowOrderColumn) % 50
      ).cast(IntegerType)
    )
  }

  def computeSecondaryPartitionIdLookup[T: Encoder](
      df: DataFrame,
      primaryPartitionColumn: String,
      secondaryPartitionColumn: String
  ): Dataset[T] = {
    df.groupBy(primaryPartitionColumn)
      .agg(max(secondaryPartitionColumn).as("maxSecondaryId"))
      // to save storage space, store only records with multiple secondary IDs
      .filter(col("maxSecondaryId") > 0)
      .sort(primaryPartitionColumn)
      .as[T]
  }

}
