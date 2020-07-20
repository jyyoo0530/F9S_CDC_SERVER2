package query

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class F9S_STATS_RAW(var spark: SparkSession, var pathSourceFrom: String,
                         var pathParquetSave: String, var pathJsonSave: String) {
  def stats_raw(): Unit = {
    lazy val FTR_DEAL = spark.read.parquet(pathSourceFrom + "/FTR_DEAL")
    lazy val FTR_DEAL_CRYR = spark.read.parquet(pathSourceFrom + "/FTR_DEAL_CRYR")
    lazy val FTR_DEAL_RTE = spark.read.parquet(pathSourceFrom + "/FTR_DEAL_RTE")
    lazy val FTR_DEAL_LINE_ITEM = spark.read.parquet(pathSourceFrom + "/FTR_DEAL_LINE_ITEM")
    lazy val MDM_CRYR = spark.read.parquet(pathSourceFrom + "/MDM_CRYR").select("CRYR_CD", "CRYR_NM")
      .withColumn("carrierCode", col("CRYR_CD"))
      .withColumn("carrierName", col("CRYR_NM"))
      .drop("CRYR_CD", "CRYR_NM")
    lazy val MDM_PORT = spark.read.parquet(pathSourceFrom + "/MDM_PORT")

    lazy val srcIdx = FTR_DEAL.select("DEAL_NR", "DEAL_CHNG_SEQ", "DEAL_DT").distinct
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

    F9S_STATS_RAW.printSchema
    F9S_STATS_RAW.write.mode("overwrite").parquet(pathParquetSave + "/F9S_STATS_RAW")

  }
}
