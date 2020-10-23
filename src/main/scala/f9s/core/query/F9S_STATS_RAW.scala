package f9s.core.query

import f9s.{appConf, hadoopConf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import scala.util.control.Breaks._

case class F9S_STATS_RAW(var spark: SparkSession) {

  def stats_raw(ftrData: List[(String, DataFrame)], mdmCryr: DataFrame): DataFrame = {

    println("////////////////////////////////F9S_STATS_RAW: JOB STARTED////////////////////////////////////////")

    lazy val FTR_DEAL = ftrData.find(_._1 == "FTR_DEAL").get._2
      .filter(col("OFER_TP_CD") === "S")
      .select(col("DEAL_NR").as("DEAL_NR"),
        col("DEAL_CHNG_SEQ").as("DEAL_CHNG_SEQ"),
        col("DEAL_DT").as("DEAL_DT"),
        col("TRDE_MKT_TP_CD").as("marketTypeCode"),
        col("OFER_RD_TRM_CD").as("rdTermCode"),
        col("OFER_PYMT_TRM_CD").as("paymentTermCode"),
        col("OFER_TP_CD").as("offerTypeCode")
      )
      println(FTR_DEAL.count)
    lazy val FTR_DEAL_CRYR = ftrData.find(_._1 == "FTR_DEAL_CRYR").get._2
      .select(col("DEAL_NR").as("DEAL_NR"),
        col("DEAL_CHNG_SEQ").as("DEAL_CHNG_SEQ"),
        col("OFER_CRYR_CD").as("carrierCode")
      )
    lazy val FTR_DEAL_RTE = ftrData.find(_._1 == "FTR_DEAL_RTE").get._2
      .select(col("DEAL_NR").as("DEAL_NR"),
        col("DEAL_CHNG_SEQ").as("DEAL_CHNG_SEQ"),
        col("OFFER_REG_SEQ").as("OFFER_REG_SEQ"),
        col("TRDE_LOC_CD").as("TRDE_LOC_CD"),
        col("TRDE_LOC_TP_CD").as("TRDE_LOC_TP_CD")
      )
    lazy val FTR_DEAL_LINE_ITEM = ftrData.find(_._1 == "FTR_DEAL_LINE_ITEM").get._2
      .select(col("DEAL_NR").as("DEAL_NR"),
        col("DEAL_CHNG_SEQ").as("DEAL_CHNG_SEQ"),
        col("BSE_YW").as("baseYearWeek"),
        col("DEAL_AMT").as("dealAmt"),
        col("DEAL_PRCE").as("dealPrice"),
        col("DEAL_QTY").as("dealQty")
      )
    var MDM_CRYR = spark.emptyDataFrame
    try {
      MDM_CRYR = ftrData.find(_._1 == "MDM_CRYR").get._2
        .select(col("CRYR_CD").as("carrierCode"), col("CRYR_NM").as("carrierName"))
    } catch {
      case e: Exception =>
        MDM_CRYR = mdmCryr
    }

    lazy val srcRte = FTR_DEAL_RTE.select("DEAL_NR", "DEAL_CHNG_SEQ", "OFFER_REG_SEQ").distinct
      .join(FTR_DEAL_RTE
        .filter(col("TRDE_LOC_TP_CD") === "02")
        .select(col("DEAL_NR"),
          col("DEAL_CHNG_SEQ"),
          col("TRDE_LOC_CD").as("polCode"),
          col("OFFER_REG_SEQ")),
        Seq("DEAL_NR", "DEAL_CHNG_SEQ", "OFFER_REG_SEQ"),
        "left")
      .join(FTR_DEAL_RTE
        .filter(col("TRDE_LOC_TP_CD") === "03")
        .select(col("DEAL_NR"),
          col("DEAL_CHNG_SEQ"),
          col("TRDE_LOC_CD").as("podCode"),
          col("OFFER_REG_SEQ")),
        Seq("DEAL_NR", "DEAL_CHNG_SEQ", "OFFER_REG_SEQ"),
        "left")

    val F9S_STATS_RAW = FTR_DEAL.join(srcRte, Seq("DEAL_NR", "DEAL_CHNG_SEQ"), "left").drop("OFFER_REG_SEQ")
      .join(FTR_DEAL_LINE_ITEM, Seq("DEAL_NR", "DEAL_CHNG_SEQ"), "left")
      .join(FTR_DEAL_CRYR, Seq("DEAL_NR", "DEAL_CHNG_SEQ"), "left")
      .join(MDM_CRYR, Seq("carrierCode"), "left")

    println(F9S_STATS_RAW.select(col("DEAL_NR")).distinct.count)
    F9S_STATS_RAW.printSchema
    println("/////////////////////////////JOB FINISHED//////////////////////////////")

    F9S_STATS_RAW
  }

  def append_stats_raw(f9sStatsRaw: DataFrame, ftrData: List[(String, DataFrame)], ftrUpdatedDataSnapshot: List[(String, DataFrame)]): DataFrame = {
    println("////////////////////////////////F9S_STATS_RAW: JOB STARTED////////////////////////////////////////")
    var FTR_DEAL = spark.emptyDataFrame
    var FTR_DEAL_CRYR = spark.emptyDataFrame
    var FTR_DEAL_RTE = spark.emptyDataFrame
    var FTR_DEAL_LINE_ITEM = spark.emptyDataFrame
    var MDM_CRYR = spark.emptyDataFrame

    // 1) make list of topic
    def tryCatch(name: String, source: List[(String, DataFrame)]): DataFrame = {
      var output = spark.emptyDataFrame
      try {
        output = source.find(_._1 == name).get._2
        output
      } catch {
        case e: NoSuchElementException =>
          System.out.println("!!!!!!" + name + "!!! No data to be updated !!!")
          output
      }
    }

    FTR_DEAL = tryCatch("FTR_DEAL", ftrUpdatedDataSnapshot)
      .filter(col("OFER_TP_CD") === "S")
    FTR_DEAL_CRYR = tryCatch("FTR_DEAL_CRYR", ftrUpdatedDataSnapshot)
    FTR_DEAL_RTE = tryCatch("FTR_DEAL_RTE", ftrUpdatedDataSnapshot)
    FTR_DEAL_LINE_ITEM = tryCatch("FTR_DEAL_LINE_ITEM", ftrUpdatedDataSnapshot)
    MDM_CRYR = ftrData.find(_._1 == "MDM_CRYR").get._2
      .select("CRYR_CD", "CRYR_NM")
      .withColumn("carrierCode", col("CRYR_CD"))
      .withColumn("carrierName", col("CRYR_NM"))
      .drop("CRYR_CD", "CRYR_NM")

    if (FTR_DEAL.count == 0 || FTR_DEAL_CRYR.count == 0 || FTR_DEAL_RTE.count == 0 || FTR_DEAL_LINE_ITEM.count == 0) {
      f9sStatsRaw
    }
    else {
      lazy val srcIdx = FTR_DEAL.select("DEAL_NR", "DEAL_CHNG_SEQ", "DEAL_DT", "OFER_TP_CD").distinct
        .join(FTR_DEAL_LINE_ITEM.select("DEAL_NR", "DEAL_CHNG_SEQ", "BSE_YW").distinct, Seq("DEAL_NR", "DEAL_CHNG_SEQ"), "left")
        .join(FTR_DEAL_RTE.select("DEAL_NR", "DEAL_CHNG_SEQ", "OFFER_REG_SEQ").distinct, Seq("DEAL_NR", "DEAL_CHNG_SEQ"), "left")
        .join(FTR_DEAL_CRYR.select("DEAL_NR", "DEAL_CHNG_SEQ", "OFER_CRYR_CD")
          .withColumn("carrierCode", col("OFER_CRYR_CD")).drop("OFER_CRYR_CD")
          .join(MDM_CRYR, Seq("carrierCode"), "left")
          .groupBy("DEAL_NR", "DEAL_CHNG_SEQ")
          .agg(collect_set(struct("carrierCode", "carrierName")).as("carrierItem")),
          Seq("DEAL_NR", "DEAL_CHNG_SEQ"), "left")
      lazy val srcRte = FTR_DEAL_RTE.select("DEAL_NR", "DEAL_CHNG_SEQ", "OFFER_REG_SEQ").distinct
        .join(FTR_DEAL_RTE
          .filter(col("TRDE_LOC_TP_CD") === "02")
          .select("DEAL_NR", "DEAL_CHNG_SEQ", "TRDE_LOC_CD", "OFFER_REG_SEQ")
          .withColumn("polCode", col("TRDE_LOC_CD")).drop("TRDE_LOC_CD"),
          Seq("DEAL_NR", "DEAL_CHNG_SEQ", "OFFER_REG_SEQ"),
          "left")
        .join(FTR_DEAL_RTE
          .filter(col("TRDE_LOC_TP_CD") === "03")
          .select("DEAL_NR", "DEAL_CHNG_SEQ", "TRDE_LOC_CD", "OFFER_REG_SEQ")
          .withColumn("podCode", col("TRDE_LOC_CD")).drop("TRDE_LOC_CD"),
          Seq("DEAL_NR", "DEAL_CHNG_SEQ", "OFFER_REG_SEQ"),
          "left")
      lazy val srcLineItem = FTR_DEAL_LINE_ITEM
        .select("DEAL_NR", "DEAL_CHNG_SEQ", "BSE_YW", "DEAL_AMT", "DEAL_PRCE", "DEAL_QTY", "OFER_PRCE", "OFER_QTY", "OFER_REMN_QTY")
        .distinct

      val F9S_STATS_RAW = srcIdx.join(srcRte, Seq("DEAL_NR", "DEAL_CHNG_SEQ", "OFFER_REG_SEQ"), "left").drop("OFFER_REG_SEQ")
        .join(srcLineItem, Seq("DEAL_NR", "DEAL_CHNG_SEQ", "BSE_YW"), "left")


      println("/////////////////////////////JOB FINISHED//////////////////////////////")

      F9S_STATS_RAW
    }


  }
}