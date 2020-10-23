package f9s.core.query

import com.mongodb.spark.MongoSpark
import f9s.{appConf, hadoopConf, mongoConf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

case class F9S_MW_SUM(var spark: SparkSession, var currentWk: String) {
  def mw_sum(srcData: DataFrame): DataFrame = {
    println("////////////////////////////////MW SUM: JOB STARTED////////////////////////////////////////")

    /////Data Load/////
    lazy val F9S_STATS_RAW = srcData
    println(F9S_STATS_RAW.select(col("DEAL_NR")).distinct().count)

    /////SQL/////
    lazy val F9S_MW_SUM = F9S_STATS_RAW
      .drop("carrierCode")
      .filter(col("offerTypeCode") === "S")
      .withColumn("containerTypeCode", lit("01"))
      .withColumn("qtyUnit", lit("T"))
      .withColumn("status", when(col("baseYearWeek") >= currentWk, lit("on Market")).otherwise(lit("Trade Closed")))
      .drop("DEAL_NR", "DEAL_CHNG_SEQ", "offerTypeCode")
      .withColumn("openChk", row_number().over(Window.partitionBy("marketTypeCode",
        "rdTermCode",
        "containerTypeCode",
        "paymentTermCode",
        "polCode",
        "podCode",
        "qtyUnit",
        "baseYearWeek",
        "status").orderBy(col("DEAL_DT").asc)))
      .withColumn("closeChk", row_number().over(Window.partitionBy("marketTypeCode",
        "rdTermCode",
        "containerTypeCode",
        "paymentTermCode",
        "polCode",
        "podCode",
        "qtyUnit",
        "baseYearWeek",
        "status").orderBy(col("DEAL_DT").desc)))
      .withColumn("open", when(col("openChk") === 1, col("dealPrice")
      ).otherwise(lit(0)))
      .withColumn("close", when(col("closeChk") === 1, col("dealPrice")
      ).otherwise(lit(0))).drop("openChk", "closeChk")
      .groupBy("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit", "baseYearWeek", "status")
      .agg(max("dealPrice").as("high"),
        min("dealPrice").as("low"),
        sum("dealQty").as("volume"),
        max("DEAL_DT").as("latestEventDate"),
        sum("open").as("open"),
        sum("close").as("close"))
      .withColumn("laggedPrice", lag(col("close"), 1, 0).over(Window.partitionBy("marketTypeCode",
        "rdTermCode",
        "containerTypeCode",
        "paymentTermCode",
        "polCode",
        "podCode",
        "qtyUnit",
        "baseYearWeek",
        "status").orderBy("baseYearWeek")))
      .withColumn("changeValue", col("close").minus(col("laggedPrice")))
      .withColumn("changeRate", col("changeValue").divide(col("close"))).distinct
      .groupBy("marketTypeCode",
        "rdTermCode",
        "containerTypeCode",
        "paymentTermCode",
        "polCode",
        "podCode",
        "qtyUnit")
      .agg(collect_set(struct("baseYearWeek",
        "status",
        "open",
        "low",
        "high",
        "close",
        "volume",
        "changeValue",
        "changeRate",
        "latestEventDate")).as("Cell")).distinct


    //////Return Result
    F9S_MW_SUM.printSchema
    println("/////////////////////////////JOB FINISHED//////////////////////////////")

    F9S_MW_SUM
  }

  def append_mw_sum(srcData:DataFrame, topicList:DataFrame): DataFrame = {
    println("///////////////////////////// MW SUM: appending JOB STARTED////////////////////////////////////")

    //// Data Load ////
    lazy val F9S_STATS_RAW = topicList.join(srcData, Seq("polCode", "podCode"), "left")

    //// SQL  ////
    lazy val F9S_MW_SUM = F9S_STATS_RAW
      .drop("carrierCode")
      .filter(col("offerTypeCode") === "S")
      .withColumn("containerTypeCode", lit("01"))
      .withColumn("qtyUnit", lit("T"))
      .withColumn("status", when(col("baseYearWeek") >= currentWk, lit("on Market")).otherwise(lit("Trade Closed")))
      .drop("DEAL_NR", "DEAL_CHNG_SEQ", "offerTypeCode")
      .withColumn("openChk", row_number().over(Window.partitionBy("marketTypeCode",
        "rdTermCode",
        "containerTypeCode",
        "paymentTermCode",
        "polCode",
        "podCode",
        "qtyUnit",
        "baseYearWeek",
        "status").orderBy(col("DEAL_DT").asc)))
      .withColumn("closeChk", row_number().over(Window.partitionBy("marketTypeCode",
        "rdTermCode",
        "containerTypeCode",
        "paymentTermCode",
        "polCode",
        "podCode",
        "qtyUnit",
        "baseYearWeek",
        "status").orderBy(col("DEAL_DT").desc)))
      .withColumn("open", when(col("openChk") === 1, col("dealPrice")
      ).otherwise(lit(0)))
      .withColumn("close", when(col("closeChk") === 1, col("dealPrice")
      ).otherwise(lit(0))).drop("openChk", "closeChk")
      .groupBy("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit", "baseYearWeek", "status")
      .agg(max("dealPrice").as("high"),
        min("dealPrice").as("low"),
        sum("dealQty").as("volume"),
        max("DEAL_DT").as("latestEventDate"),
        sum("open").as("open"),
        sum("close").as("close"))
      .withColumn("laggedPrice", lag(col("close"), 1, 0).over(Window.partitionBy("marketTypeCode",
        "rdTermCode",
        "containerTypeCode",
        "paymentTermCode",
        "polCode",
        "podCode",
        "qtyUnit",
        "baseYearWeek",
        "status").orderBy("baseYearWeek")))
      .withColumn("changeValue", col("close").minus(col("laggedPrice")))
      .withColumn("changeRate", col("changeValue").divide(col("close"))).distinct
      .groupBy("marketTypeCode",
        "rdTermCode",
        "containerTypeCode",
        "paymentTermCode",
        "polCode",
        "podCode",
        "qtyUnit")
      .agg(collect_set(struct("baseYearWeek",
        "status",
        "open",
        "low",
        "high",
        "close",
        "volume",
        "changeValue",
        "changeRate",
        "latestEventDate")).as("Cell"))


    //////Return Result
    println("/////////////////////////////JOB FINISHED//////////////////////////////")

    F9S_MW_SUM
  }
}
