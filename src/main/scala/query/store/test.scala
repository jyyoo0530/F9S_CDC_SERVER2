package query.store

import org.apache.spark.sql.SparkSession
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}

case class test(var spark: SparkSession, var pathSourceFrom: String,
                var pathParquetSave: String, var pathJsonSave: String, var currentWk: String) {
  def test(): Unit = {
    lazy val FTR_DEAL = spark.read.parquet(pathSourceFrom + "/FTR_DEAL")
      .select(col("DEAL_NR").as("dealNumber"),
        col("DEAL_CHNG_SEQ").as("dealChangeSeq"),
        col("TRDE_MKT_TP_CD").as("marketTypeCode"),
        col("OFER_PYMT_TRM_CD").as("paymentTermCode"),
        col("OFER_RD_TRM_CD").as("rdTermCode"),
        col("DEAL_DT").as("timestamp")
      )
    lazy val F9S_STATS_RAW = spark.read.parquet(pathParquetSave + "/F9S_STATS_RAW")
      .select(
        col("DEAL_NR").as("dealNumber"),
        col("DEAL_CHNG_SEQ").as("dealChangeSeq"),
        col("BSE_YW").as("baseYearWeek"),
        col("DEAL_QTY").as("volume"),
        col("DEAL_PRCE").as("dealPrice"),
        col("polCode"),
        col("podCode"))
      .withColumn("containerTypeCode", lit("01"))
      .withColumn("qtyUnit", lit("T"))
      .filter(col("dealPrice") =!= 0)

    lazy val src01 =
      F9S_STATS_RAW.join(FTR_DEAL, Seq("dealNumber", "dealChangeSeq"), "left").distinct
        .withColumn("xAxis", concat(col("timestamp").substr(lit(1), lit(4)),
          weekofyear(to_timestamp(col("timestamp").substr(lit(1), lit(8)), "yyyyMMdd"))))
//
      src01.printSchema
      src01.orderBy("polCode", "podCode", "baseYearWeek", "xAxis", "timestamp").show(30)
//        .withColumn("interval", lit("weekly")) /// 루프포인트
//        .withColumn("openChk", row_number.over(Window.partitionBy(col("marketTypeCode"),
//          col("rdTermCode"),
//          col("containerTypeCode"),
//          col("paymentTermCode"),
//          col("polCode"),
//          col("podCode"),
//          col("baseYearWeek"),
//          col("xAxis")).orderBy(col("timestamp").asc)))
//        .withColumn("closeChk", row_number.over(Window.partitionBy(col("marketTypeCode"),
//          col("rdTermCode"),
//          col("containerTypeCode"),
//          col("paymentTermCode"),
//          col("polCode"),
//          col("podCode"),
//          col("baseYearWeek"),
//          col("xAxis")).orderBy(col("timestamp").desc)))
//        .withColumn("open", when(col("openChk") === 1, col("dealPrice")).otherwise(lit(0))).drop("openChk")
//        .withColumn("close", when(col("closeChk") === 1, col("dealPrice")).otherwise(lit(0))).drop("closeChk")
//        .groupBy(col("marketTypeCode"),
//          col("rdTermCode"),
//          col("containerTypeCode"),
//          col("paymentTermCode"),
//          col("polCode"),
//          col("podCode"),
//          col("qtyUnit"),
//          col("baseYearWeek"),
//          col("interval"),
//          col("xAxis"))
//        .agg(max("dealPrice").as("high"),
//          min("dealPrice").as("low"),
//          max("open").as("open"),
//          max("close").as("close"),
//          sum("volume").as("volume"),
//          max("timestamp").as("intervalTimestamp"))
//        .withColumn("laggedPrice", lag(col("close"), 1, 0).over(Window.partitionBy(
//          col("marketTypeCode"),
//          col("rdTermCode"),
//          col("containerTypeCode"),
//          col("paymentTermCode"),
//          col("polCode"),
//          col("podCode"),
//          col("qtyUnit"),
//          col("baseYearWeek"),
//          col("interval")
//        ).orderBy(col("intervalTimestamp").asc)))
//        .withColumn("changeValue", col("close").minus(col("laggedPrice")))
//        .withColumn("changeRate", col("changeValue").divide(col("close")))
//        .distinct.drop("laggedPrice")
//
//    //    src01.orderBy("polCode", "podCode", "baseYearWeek", "interval","xAxis").show(10)
//    val F9S_MW_WKDETAIL = src01.groupBy(col("marketTypeCode"),
//        col("rdTermCode"),
//        col("containerTypeCode"),
//        col("paymentTermCode"),
//        col("polCode"),
//        col("podCode"),
//        col("qtyUnit"),
//        col("baseYearWeek"),
//        col("interval"))
//      .agg(collect_set(struct("xAxis","intervalTimestamp", "open", "low", "high", "close", "volume", "changeValue", "changeRate")).as("Cell"))

  }
}
