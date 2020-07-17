package query

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

case class F9S_DSBD_WKLIST(var spark: SparkSession, var pathSourceFrom: String, var pathParquetSave: String, var pathJsonSave: String) {
  def dsbd_wklist(): Unit = {
    lazy val FTR_OFER: DataFrame = spark.read.parquet(pathSourceFrom + "/FTR_OFER")
    lazy val FTR_OFER_RTE: DataFrame = spark.read.parquet(pathSourceFrom + "/FTR_OFER_RTE")
    lazy val FTR_OFER_LINE_ITEM: DataFrame = spark.read.parquet(pathSourceFrom + "/FTR_OFER_LINE_ITEM")

    lazy val polRte: DataFrame = FTR_OFER_RTE.filter(col("TRDE_LOC_TP_CD") === "02").select("TRDE_LOC_CD", "OFER_CHNG_SEQ", "OFER_NR", "OFER_REG_SEQ").withColumn("polCode", col("TRDE_LOC_CD")).drop("TRDE_LOC_CD")
    lazy val podRte: DataFrame = FTR_OFER_RTE.filter(col("TRDE_LOC_TP_CD") === "03").select("TRDE_LOC_CD", "OFER_CHNG_SEQ", "OFER_NR", "OFER_REG_SEQ").withColumn("podCode", col("TRDE_LOC_CD")).drop("TRDE_LOC_CD")
    lazy val srcRte: DataFrame = polRte.join(podRte, Seq("OFER_NR", "OFER_CHNG_SEQ", "OFER_REG_SEQ"), "left")
      .withColumn("offerNumber", col("OFER_NR"))
      .withColumn("offerChangeSeq", col("OFER_CHNG_SEQ"))
      .drop("OFER_NR", "OFER_CHNG_SEQ", "OFER_REG_SEQ")
      .groupBy("polCode", "podCode", "offerNumber").agg(max("offerChangeSeq").as("offerChangeSeq"))
    lazy val srcLineItem: DataFrame = FTR_OFER_LINE_ITEM.select("BSE_YW", "OFER_NR", "OFER_CHNG_SEQ", "OFER_REMN_QTY").groupBy("OFER_NR", "BSE_YW").agg(max("OFER_CHNG_SEQ").as("offerChangeSeq")).withColumn("offerNumber", col("OFER_NR")).drop("OFER_NR")
    lazy val srcOfer: DataFrame = FTR_OFER.select("EMP_NR", "OFER_NR", "OFER_CHNG_SEQ", "OFER_TP_CD").groupBy("OFER_NR", "OFER_TP_CD").agg(max("OFER_CHNG_SEQ").as("offerChangeSeq"), first("EMP_NR").as("userId")).withColumn("offerNumber", col("OFER_NR")).drop("OFER_NR").withColumn("offerTypeCode", col("OFER_TP_CD")).drop("OFER_TP_CD")

    val agged1: DataFrame = srcOfer.join(srcLineItem, Seq("offerNumber", "offerChangeSeq"), "left").join(srcRte, Seq("offerNumber", "offerChangeSeq"), "left").groupBy("userId", "offerTypeCode").agg(collect_set("BSE_YW").as("baseYearWeek")).withColumn("polCode", lit("all")).withColumn("podCode", lit("all")).select("userId", "offerTypeCode","polCode", "podCode", "baseYearWeek")
    val agged2: DataFrame = srcOfer.join(srcLineItem, Seq("offerNumber", "offerChangeSeq"), "left").join(srcRte, Seq("offerNumber", "offerChangeSeq"), "left").groupBy("userId", "offerTypeCode", "polCode").agg(collect_set("BSE_YW").as("baseYearWeek")).withColumn("podCode", lit("all")).select("userId", "offerTypeCode","polCode", "podCode", "baseYearWeek")
    val agged3: DataFrame = srcOfer.join(srcLineItem, Seq("offerNumber", "offerChangeSeq"), "left").join(srcRte, Seq("offerNumber", "offerChangeSeq"), "left").groupBy("userId", "offerTypeCode", "podCode").agg(collect_set("BSE_YW").as("baseYearWeek")).withColumn("polCode", lit("all")).select("userId", "offerTypeCode","polCode", "podCode", "baseYearWeek")
    val agged4: DataFrame = srcOfer.join(srcLineItem, Seq("offerNumber", "offerChangeSeq"), "left").join(srcRte, Seq("offerNumber", "offerChangeSeq"), "left").groupBy("userId", "offerTypeCode", "polCode", "podCode").agg(collect_set("BSE_YW").as("baseYearWeek"))
    agged1.printSchema
    agged2.printSchema
    agged3.printSchema
    agged4.printSchema
    val F9S_DSBD_WKLIST = agged1.union(agged2).union(agged3).union(agged4).sort(col("userId").desc)
      .groupBy("userId", "offerTypeCode", "polCode", "podCode", "baseYearWeek")
      .agg(lit("dummy")).distinct.drop("dummy")

    lazy val mwidx: DataFrame = F9S_DSBD_WKLIST.select("userId", "offerTypeCode")
    lazy val idx = mwidx.collect()
    lazy val userId: Array[String] = mwidx.select("userId").collect().map(_ (0).toString)
    lazy val offerTypeCode: Array[String] = mwidx.select("offerTypeCode").collect().map(_ (0).toString)

    lazy val tgData: DataFrame = F9S_DSBD_WKLIST

    for (i <- idx.indices) {
      tgData.filter(
        col("userId") === userId(i) &&
          col("offerTypeCode") === offerTypeCode(i)
      )
        .drop("rteIdx")
        .write.mode("append").json(pathJsonSave + "/F9S_DSBD_WKLIST" + "/" + userId(i) + "/" + offerTypeCode(i))
    }

    F9S_DSBD_WKLIST.write.mode("append").parquet(pathParquetSave + "/F9S_DSBD_WKLIST")
  }
}