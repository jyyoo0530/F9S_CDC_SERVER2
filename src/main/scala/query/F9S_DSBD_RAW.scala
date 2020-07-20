package query

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


case class F9S_DSBD_RAW(var spark: SparkSession, var pathSourceFrom: String,
                          var pathParquetSave: String, var pathJsonSave: String){
  def dsbd_raw(): Unit ={
    lazy val FTR_OFER = spark.read.parquet(pathSourceFrom + "/FTR_OFER")
    lazy val FTR_OFER_CRYR = spark.read.parquet(pathSourceFrom + "/FTR_OFER_CRYR")
    lazy val FTR_OFER_RTE = spark.read.parquet(pathSourceFrom + "/FTR_OFER_RTE")
    lazy val FTR_OFER_LINE_ITEM = spark.read.parquet(pathSourceFrom + "/FTR_OFER_LINE_ITEM")
    lazy val MDM_CRYR = spark.read.parquet(pathSourceFrom + "/MDM_CRYR")
      .select(col("CRYR_CD").as("carrierCode"), col("CRYR_NM").as("carrierName"))
    lazy val MDM_PORT = spark.read.parquet(pathSourceFrom + "/MDM_PORT")

    lazy val srcIdx = FTR_OFER.select("OFER_NR", "OFER_CHNG_SEQ", "OFER_TP_CD", "EMP_NR", "ALL_YN").distinct
      .join(FTR_OFER_LINE_ITEM.select("OFER_NR", "OFER_CHNG_SEQ", "BSE_YW").distinct, Seq("OFER_NR", "OFER_CHNG_SEQ"), "left")
      .join(FTR_OFER_RTE.select("OFER_NR", "OFER_CHNG_SEQ", "OFER_REG_SEQ").distinct, Seq("OFER_NR", "OFER_CHNG_SEQ"), "left")
      .join(FTR_OFER_CRYR.select("OFER_NR", "OFER_CHNG_SEQ", "OFER_CRYR_CD")
        .withColumn("carrierCode", col("OFER_CRYR_CD")).drop("OFER_CRYR_CD")
        .join(MDM_CRYR, Seq("carrierCode"), "left")
        .groupBy("OFER_NR", "OFER_CHNG_SEQ")
        .agg(countDistinct("carrierCode").as("carrierCount"), collect_set(struct("carrierCode", "carrierName")).as("carrierItem")),
        Seq("OFER_NR", "OFER_CHNG_SEQ"), "left")
    lazy val srcRte = FTR_OFER_RTE.select("OFER_NR", "OFER_CHNG_SEQ", "OFER_REG_SEQ").distinct
      .join(FTR_OFER_RTE
        .filter(col("TRDE_LOC_TP_CD") === "02")
        .select("OFER_NR", "OFER_CHNG_SEQ", "TRDE_LOC_CD", "OFER_REG_SEQ")
        .withColumn("polCode", col("TRDE_LOC_CD")).drop("TRDE_LOC_CD"),
        Seq("OFER_NR", "OFER_CHNG_SEQ", "OFER_REG_SEQ"),
        "left")
      .join(FTR_OFER_RTE
        .filter(col("TRDE_LOC_TP_CD") === "03")
        .select("OFER_NR", "OFER_CHNG_SEQ", "TRDE_LOC_CD", "OFER_REG_SEQ")
        .withColumn("podCode", col("TRDE_LOC_CD")).drop("TRDE_LOC_CD"),
        Seq("OFER_NR", "OFER_CHNG_SEQ", "OFER_REG_SEQ"),
        "left")
      .join(MDM_PORT.select(col("locCd").as("polCode"), col("locNm").as("polName")), Seq("polCode"), "left")
      .join(MDM_PORT.select(col("locCd").as("podCode"), col("locNm").as("podName")), Seq("podCode"), "left")
    lazy val srcLineItem = FTR_OFER_LINE_ITEM
      .select("OFER_NR", "OFER_CHNG_SEQ", "BSE_YW", "DEAL_AMT", "DEAL_PRCE", "DEAL_QTY", "OFER_PRCE", "OFER_QTY", "OFER_REMN_QTY")
      .distinct

    val F9S_DSBD_RAW = srcIdx.join(srcRte, Seq("OFER_NR", "OFER_CHNG_SEQ", "OFER_REG_SEQ"), "left").drop("OFER_REG_SEQ")
      .join(srcLineItem, Seq("OFER_NR", "OFER_CHNG_SEQ", "BSE_YW"), "left")

    F9S_DSBD_RAW.printSchema
    F9S_DSBD_RAW.write.mode("overwrite").parquet(pathParquetSave + "/F9S_DSBD_RAW")
//    F9S_DSBD_RAW.write.partitionBy(20).mode("overwrite").json(pathJsonSave + "/F9S_DSBD_RAW")
  }
}
