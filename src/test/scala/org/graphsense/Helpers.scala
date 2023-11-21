package org.graphsense

import org.apache.spark.sql.{
  DataFrame,
  Dataset,
  Encoder,
  Encoders,
  SparkSession
}
import org.apache.spark.sql.catalyst.ScalaReflection.universe.TypeTag
import org.apache.spark.sql.functions.{col, length, lit, udf, when}
import org.apache.spark.sql.types.{
  ArrayType,
  MapType,
  StringType,
  StructField,
  StructType
}
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.Column
import org.graphsense.account.Implicits._

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("Transformation Test")
      .config("spark.sql.shuffle.partitions", "3")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
  }
}

object Helpers {

  def readTestData[T <: Product: Encoder: TypeTag](
      spark: SparkSession,
      file: String
  ): Dataset[T] = {
    val schema = Encoders.product[T].schema
    // spark.read.csv cannot read BinaryType, read BinaryType as StringType and cast to ByteArray
    val newSchema = StructType(
      schema.map(x =>
        if (x.dataType.toString == "BinaryType")
          StructField(x.name, StringType, true)
        else (
          if (x.dataType.toString == "ArrayType(BinaryType,true)")
            StructField(x.name, ArrayType(StringType), true)
          else StructField(x.name, x.dataType, true)
        )
      )
    )

    val binaryColumns = schema.collect {
      case x if x.dataType.toString == "BinaryType" => x.name
    }
    val binaryArrayColumns = schema.collect {
      case x if x.dataType.toString == "ArrayType(BinaryType,true)" => x.name
    }

    val hexStringToByteArray = udf((x: String) =>
      x.grouped(2).toArray map { Integer.parseInt(_, 16).toByte }
    )

    val hatba = udf((x: Seq[String]) => x.map(hexStrToBytes).toArray)

    val fileSuffix = file.toUpperCase.split("\\.").last
    val df =
      if (fileSuffix == "JSON") spark.read.schema(newSchema).json(file)
      else spark.read.schema(newSchema).option("header", true).csv(file)

    val df2 = binaryColumns
      .foldLeft(df) { (curDF, colName) =>
        curDF.withColumn(
          colName,
          when(
            col(colName).isNotNull,
            hexStringToByteArray(
              col(colName).substr(lit(3), length(col(colName)) - 2)
            )
          )
        )
      }
    val df3 = binaryArrayColumns
      .foldLeft(df2) { (curDF, colName) =>
        curDF.withColumn(
          colName,
          when(
            col(colName).isNotNull,
            hatba(col(colName))
          )
        )
      }

    df3.as[T]
  }

}

abstract class TestBase
    extends AnyFunSuite
    with SparkSessionTestWrapper
    with DataFrameComparer {

  def readTestData[T <: Product: Encoder: TypeTag](file: String): Dataset[T] = {
    Helpers.readTestData(spark, file)
  }

  def setNullableStateForAllColumns[T](
      ds: Dataset[T],
      nullable: Boolean = true,
      containsNull: Boolean = true
  ): DataFrame = {
    def set(st: StructType): StructType = {
      StructType(st.map { case StructField(name, dataType, _, metadata) =>
        val newDataType = dataType match {
          case t: StructType          => set(t)
          case ArrayType(dataType, _) => ArrayType(dataType, containsNull)
          case MapType(kt, dataType: StructType, _) =>
            MapType(kt, set(dataType), containsNull)
          case MapType(kt, dataType, _) => MapType(kt, dataType, containsNull)
          case _                        => dataType
        }
        StructField(name, newDataType, nullable = nullable, metadata)
      })
    }
    ds.sqlContext.createDataFrame(ds.toDF.rdd, set(ds.schema))
  }

  def assertDataFrameEquality[T](
      actualDS: Dataset[T],
      expectedDS: Dataset[T]
  ): Unit = {
    val colOrder: Array[Column] = expectedDS.columns.map(col)

    assertSmallDataFrameEquality(
      setNullableStateForAllColumns(actualDS.select(colOrder: _*)),
      setNullableStateForAllColumns(expectedDS)
    )
  }

}
