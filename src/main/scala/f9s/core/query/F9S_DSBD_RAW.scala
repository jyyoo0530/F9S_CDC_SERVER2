package f9s.core.query


import f9s.{appConf, hadoopConf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


case class F9S_DSBD_RAW(var spark: SparkSession) {
  val filePath =  appConf().dataLake match {
    case "file" => appConf().folderOrigin
    case "hadoop" => hadoopConf.hadoopPath
  }

  def dsbd_raw(): Unit = {
    lazy val FTR_OFER = spark.read.parquet(filePath + "/FTR_OFER")
    lazy val FTR_OFER_CRYR = spark.read.parquet(filePath + "/FTR_OFER_CRYR")
    lazy val FTR_OFER_RTE = spark.read.parquet(filePath + "/FTR_OFER_RTE")
    lazy val FTR_OFER_LINE_ITEM = spark.read.parquet(filePath + "/FTR_OFER_LINE_ITEM")
    lazy val MDM_CRYR = spark.read.parquet(filePath + "/MDM_CRYR")
      .select(col("CRYR_CD").as("carrierCode"), col("CRYR_NM").as("carrierName"))
    lazy val MDM_PORT = spark.read.parquet(filePath + "/MDM_PORT")

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
    F9S_DSBD_RAW.write.mode("overwrite").parquet(filePath
      + "/F9S_DSBD_RAW")
  }

  def append_dsbd_raw(FTR_OFER: DataFrame, FTR_OFER_CRYR: DataFrame, FTR_OFER_RTE: DataFrame, FTR_OFER_LINE_ITEM: DataFrame): DataFrame = {
    println("<------------DSBD RAW: Loading Instruction STARTED------------>")
    lazy val MDM_CRYR = spark.read.parquet(filePath + "/MDM_CRYR")
      .select(col("CRYR_CD").as("carrierCode"), col("CRYR_NM").as("carrierName"))
    lazy val MDM_PORT = spark.read.parquet(filePath + "/MDM_PORT")

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
    F9S_DSBD_RAW.write.mode("append").parquet(filePath + "/F9S_DSBD_RAW")
    println("<------------Loading Instruction FINISHED------------>")
    return F9S_DSBD_RAW
  }
}
