package query

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class F9S_IDX_LST(var spark: SparkSession, var pathParquetSave: String, var pathJsonSave: String){
  def idx_lst(): Unit ={
    val F9S_MI_SUM = spark.read.parquet(pathParquetSave+"/F9S_MI_SUM")
    val schema = StructType(List(
      StructField("intervalSeq", IntegerType, nullable = false),
      StructField("interval", StringType, nullable = false)
    ))
    val rdd = spark.sparkContext.parallelize(Seq(
      Row(1, "tick"),
      Row(2, "secondly"),
      Row(3, "10sec"),
      Row(4, "30sec"),
      Row(5, "minutely"),
      Row(6, "5min"),
      Row(7, "10min"),
      Row(8, "30min"),
      Row(9, "hourly"),
      Row(10, "daily"),
      Row(11, "weekly"),
      Row(12, "monthly"),
      Row(13, "yearly")
    ))
    val intervalDf = spark.createDataFrame(rdd, schema)

    val F9S_IDX_LST=F9S_MI_SUM
      .join(intervalDf, Seq("interval"), "left")
      .sort(col("intervalSeq").asc).distinct
      .groupBy("idxSubject", "idxCategory", "idxCd", "idxNm")
      .agg(collect_list(struct("intervalSeq", "interval")).as("intervalItem"))

    F9S_IDX_LST.write.mode("append").parquet(pathParquetSave+"/F9S_IDX_LST")
    F9S_IDX_LST.write.mode("append").json(pathJsonSave+"/F9S_IDX_LST")
  }
}