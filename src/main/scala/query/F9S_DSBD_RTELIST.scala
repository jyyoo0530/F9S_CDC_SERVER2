package query

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class F9S_DSBD_RTELIST(var spark: SparkSession, var pathSourceFrom: String,
                            var pathParquetSave: String, var pathJsonSave: String) {
  def dsbd_rtelist(): Unit = {
    lazy val FTR_OFER = spark.read.parquet(pathSourceFrom + "/FTR_OFER")
    lazy val FTR_OFER_RTE = spark.read.parquet(pathSourceFrom + "/FTR_OFER_RTE")
    lazy val MDM_PORT = spark.read.parquet(pathSourceFrom + "/MDM_PORT")

    lazy val srcMdmPort = MDM_PORT.select(col("locCd"), col("locNm")).distinct
    lazy val srcOfer = FTR_OFER.select(
      col("OFER_NR").as("offerNumber"),
      col("EMP_NR").as("userId"),
      col("OFER_TP_CD").as("offerTypeCode")
    )
    lazy val srcPol = FTR_OFER_RTE.select(
      col("OFER_NR").as("offerNumber"),
      col("TRDE_LOC_CD").as("polCode"),
      col("TRDE_LOC_TP_CD"),
      col("OFER_REG_SEQ")
    )
      .filter(col("TRDE_LOC_TP_CD") === "02")
      .drop("TRDE_LOC_TP_CD")
      .join(
        srcMdmPort.withColumn("polCode", col("locCd"))
          .withColumn("polName", col("locNm"))
          .drop("locCd", "locNm"), Seq("polCode"), "left"
      )
    lazy val srcPod = FTR_OFER_RTE.select(
      col("OFER_NR").as("offerNumber"),
      col("TRDE_LOC_CD").as("podCode"),
      col("TRDE_LOC_TP_CD"),
      col("OFER_REG_SEQ")
    )
      .filter(col("TRDE_LOC_TP_CD") === "03")
      .drop("TRDE_LOC_TP_CD")
      .join(
        srcMdmPort.withColumn("podCode", col("locCd"))
          .withColumn("podName", col("locNm"))
          .drop("locCd", "locNm"), Seq("podCode"), "left"
      )
    lazy val srcRte = srcPol.join(srcPod, Seq("offerNumber", "OFER_REG_SEQ"), "left").drop("OFER_REG_SEQ")

    lazy val agged1 = srcOfer.join(srcRte, Seq("offerNumber"), "left").drop("offerNumber").distinct
    lazy val F9S_DSBD_RTELIST = agged1.groupBy("userId", "offerTypeCode").agg(collect_list(struct("polCode", "podCode", "polName", "podName")).as("rteList"))

    lazy val mwidx = F9S_DSBD_RTELIST.select("userId", "offerTypeCode")
    lazy val idx = mwidx.collect()
    lazy val userId = mwidx.select("userId").collect().map(_ (0).toString)
    lazy val offerTypeCode = mwidx.select("offerTypeCode").collect().map(_ (0).toString)

    lazy val tgData = F9S_DSBD_RTELIST

    for (i <- idx.indices) {
      tgData.filter(
        col("userId") === userId(i) && col("offerTypeCode") === offerTypeCode(i)
      )
        .write.mode("overwrite").json(pathJsonSave + "/F9S_DSBD_RTELIST" + "/" + userId(i) + "/" + offerTypeCode(i))
    }

    F9S_DSBD_RTELIST.write.mode("overwrite").parquet(pathParquetSave + "/F9S_DSBD_RTELIST")
  }
}
