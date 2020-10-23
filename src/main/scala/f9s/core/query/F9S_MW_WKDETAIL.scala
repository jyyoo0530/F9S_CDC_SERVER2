package f9s.core.query

import com.mongodb.spark.MongoSpark
import f9s.{appConf, hadoopConf, mongoConf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

case class F9S_MW_WKDETAIL(var spark: SparkSession) {

  val filePath: String = appConf().dataLake match {
    case "file" => appConf().folderOrigin
    case "hadoop" => hadoopConf.hadoopPath
  }

  def mw_wkdetail(srcData: DataFrame): DataFrame = {
    println("////////////////////////////////MW WKDETAIL: JOB STARTED////////////////////////////////////////")
    /////////DATA LOAD/////////////
    lazy val F9S_STATS_RAW = srcData

    ///////SQL////////////
    var srcAgg = spark.emptyDataFrame
    lazy val interval = List("daily", "weekly", "monthly")

    def grantInterval2DF(src: DataFrame, interval: String): DataFrame = {
      val srcGenerated = src
        .drop("carrierCode")
        .withColumn("containerTypeCode", lit("01"))
        .withColumn("timestamp", col("DEAL_DT"))
        .withColumn("volume", col("dealQty"))
        .withColumn("qtyUnit", lit("T")).drop("DEAL_NR", "DEAL_CHNG_SEQ", "DEAL_DT", "dealQty")
        .filter(col("dealPrice") =!= 0)
      var srcXadded = spark.emptyDataFrame
      interval match {
        case "daily" => {
          srcXadded = srcGenerated
            .withColumn("xAxis", col("timestamp").substr(lit(1), lit(8)))
            .withColumn("interval", lit("daily"))
        }
        case "weekly" => {
          srcXadded = srcGenerated
            .withColumn("xAxis", concat(col("timestamp").substr(lit(1), lit(4)),
              weekofyear(to_timestamp(col("timestamp").substr(lit(1), lit(8)), "yyyyMMdd"))))
            .withColumn("interval", lit("weekly"))
        }
        case "monthly" => {
          srcXadded = srcGenerated
            .withColumn("xAxis", col("timestamp").substr(lit(1), lit(6)))
            .withColumn("interval", lit("monthly"))
        }
      }
      val result = srcXadded.withColumn("openChk", row_number.over(Window.partitionBy(col("marketTypeCode"),
        col("rdTermCode"),
        col("containerTypeCode"),
        col("paymentTermCode"),
        col("polCode"),
        col("podCode"),
        col("baseYearWeek"),
        col("xAxis")).orderBy(col("timestamp").asc)))
        .withColumn("closeChk", row_number.over(Window.partitionBy(col("marketTypeCode"),
          col("rdTermCode"),
          col("containerTypeCode"),
          col("paymentTermCode"),
          col("polCode"),
          col("podCode"),
          col("baseYearWeek"),
          col("xAxis")).orderBy(col("timestamp").desc)))
        .withColumn("open", when(col("openChk") === 1, col("dealPrice")).otherwise(lit(0))).drop("openChk")
        .withColumn("close", when(col("closeChk") === 1, col("dealPrice")).otherwise(lit(0))).drop("closeChk")
        .groupBy(col("marketTypeCode"),
          col("rdTermCode"),
          col("containerTypeCode"),
          col("paymentTermCode"),
          col("polCode"),
          col("podCode"),
          col("qtyUnit"),
          col("baseYearWeek"),
          col("interval"),
          col("xAxis"))
        .agg(max("dealPrice").as("high"),
          min("dealPrice").as("low"),
          max("open").as("open"),
          max("close").as("close"),
          sum("volume").as("volume"),
          max("timestamp").as("intervalTimestamp"))
        .withColumn("laggedPrice", lag(col("close"), 1, 0).over(Window.partitionBy(
          col("marketTypeCode"),
          col("rdTermCode"),
          col("containerTypeCode"),
          col("paymentTermCode"),
          col("polCode"),
          col("podCode"),
          col("baseYearWeek")).orderBy(col("intervalTimestamp").asc)))
        .withColumn("changeValue", col("close").minus(col("laggedPrice")))
        .withColumn("changeRate", col("changeValue").divide(col("close")))
        .distinct.drop("laggedPrice")
      result
    }

    srcAgg = grantInterval2DF(F9S_STATS_RAW, "daily")
      .union(
        grantInterval2DF(F9S_STATS_RAW, "weekly")
      ).union(
      grantInterval2DF(F9S_STATS_RAW, "monthly")
    ).distinct

    val F9S_MW_WKDETAIL = srcAgg.groupBy(col("marketTypeCode"),
      col("rdTermCode"),
      col("containerTypeCode"),
      col("paymentTermCode"),
      col("polCode"),
      col("podCode"),
      col("qtyUnit"),
      col("baseYearWeek"),
      col("interval"))
      .agg(collect_set(struct("xAxis", "intervalTimestamp", "open", "low", "high", "close", "volume", "changeValue", "changeRate")).as("Cell"))

    F9S_MW_WKDETAIL.printSchema

    println("/////////////////////////////JOB FINISHED//////////////////////////////")

    F9S_MW_WKDETAIL
  }

  def append_mw_wkdetail(srcData: DataFrame, topicList: DataFrame): DataFrame = {
    println("////////////////////////////////MW WKDETAIL: JOB STARTED////////////////////////////////////////")
    /////////DATA LOAD/////////////
    lazy val F9S_STATS_RAW = topicList.join(srcData, Seq("polCode", "podCode", "baseYearWeek"), "left")

    ///////SQL////////////
    var srcAgg = spark.emptyDataFrame
    lazy val interval = List("daily", "weekly", "monthly")

    def grantInterval2DF(src: DataFrame, interval: String): DataFrame = {
      val srcGenerated = src
        .drop("carrierCode")
        .withColumn("containerTypeCode", lit("01"))
        .withColumn("timestamp", col("DEAL_DT"))
        .withColumn("volume", col("dealQty"))
        .withColumn("qtyUnit", lit("T")).drop("DEAL_NR", "DEAL_CHNG_SEQ", "DEAL_DT", "dealQty")
        .filter(col("dealPrice") =!= 0)
      var srcXadded = spark.emptyDataFrame
      interval match {
        case "daily" => {
          srcXadded = srcGenerated
            .withColumn("xAxis", col("timestamp").substr(lit(1), lit(8)))
            .withColumn("interval", lit("daily"))
        }
        case "weekly" => {
          srcXadded = srcGenerated
            .withColumn("xAxis", concat(col("timestamp").substr(lit(1), lit(4)),
              weekofyear(to_timestamp(col("timestamp").substr(lit(1), lit(8)), "yyyyMMdd"))))
            .withColumn("interval", lit("weekly"))
        }
        case "monthly" => {
          srcXadded = srcGenerated
            .withColumn("xAxis", col("timestamp").substr(lit(1), lit(6)))
            .withColumn("interval", lit("monthly"))
        }
      }
      val result = srcXadded.withColumn("openChk", row_number.over(Window.partitionBy(col("marketTypeCode"),
        col("rdTermCode"),
        col("containerTypeCode"),
        col("paymentTermCode"),
        col("polCode"),
        col("podCode"),
        col("baseYearWeek"),
        col("xAxis")).orderBy(col("timestamp").asc)))
        .withColumn("closeChk", row_number.over(Window.partitionBy(col("marketTypeCode"),
          col("rdTermCode"),
          col("containerTypeCode"),
          col("paymentTermCode"),
          col("polCode"),
          col("podCode"),
          col("baseYearWeek"),
          col("xAxis")).orderBy(col("timestamp").desc)))
        .withColumn("open", when(col("openChk") === 1, col("dealPrice")).otherwise(lit(0))).drop("openChk")
        .withColumn("close", when(col("closeChk") === 1, col("dealPrice")).otherwise(lit(0))).drop("closeChk")
        .groupBy(col("marketTypeCode"),
          col("rdTermCode"),
          col("containerTypeCode"),
          col("paymentTermCode"),
          col("polCode"),
          col("podCode"),
          col("qtyUnit"),
          col("baseYearWeek"),
          col("interval"),
          col("xAxis"))
        .agg(max("dealPrice").as("high"),
          min("dealPrice").as("low"),
          max("open").as("open"),
          max("close").as("close"),
          sum("volume").as("volume"),
          max("timestamp").as("intervalTimestamp"))
        .withColumn("laggedPrice", lag(col("close"), 1, 0).over(Window.partitionBy(
          col("marketTypeCode"),
          col("rdTermCode"),
          col("containerTypeCode"),
          col("paymentTermCode"),
          col("polCode"),
          col("podCode"),
          col("baseYearWeek")).orderBy(col("intervalTimestamp").asc)))
        .withColumn("changeValue", col("close").minus(col("laggedPrice")))
        .withColumn("changeRate", col("changeValue").divide(col("close")))
        .distinct.drop("laggedPrice")
      result
    }

    srcAgg = grantInterval2DF(F9S_STATS_RAW, "daily")
      .union(
        grantInterval2DF(F9S_STATS_RAW, "weekly")
      ).union(
      grantInterval2DF(F9S_STATS_RAW, "monthly")
    ).distinct

    val F9S_MW_WKDETAIL = srcAgg.groupBy(col("marketTypeCode"),
      col("rdTermCode"),
      col("containerTypeCode"),
      col("paymentTermCode"),
      col("polCode"),
      col("podCode"),
      col("qtyUnit"),
      col("baseYearWeek"),
      col("interval"))
      .agg(collect_set(struct("xAxis", "intervalTimestamp", "open", "low", "high", "close", "volume", "changeValue", "changeRate")).as("Cell"))
      .sort(col("interval").asc)


    println("/////////////////////////////JOB FINISHED//////////////////////////////")

    F9S_MW_WKDETAIL
  }
}

