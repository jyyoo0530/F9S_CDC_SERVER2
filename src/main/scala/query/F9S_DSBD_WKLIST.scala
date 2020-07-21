package query

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

case class F9S_DSBD_WKLIST(var spark: SparkSession, var pathSourceFrom: String, var pathParquetSave: String, var pathJsonSave: String) {
  def dsbd_wklist(): Unit = {
    println("////////////////////////////////DSBD WKLIST: JOB STARTED////////////////////////////////////////")
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

    val agged1: DataFrame = srcOfer.join(srcLineItem, Seq("offerNumber", "offerChangeSeq"), "left").join(srcRte, Seq("offerNumber", "offerChangeSeq"), "left").groupBy("userId", "offerTypeCode").agg(collect_set("BSE_YW").as("baseYearWeek")).withColumn("polCode", lit("all")).withColumn("podCode", lit("all")).select("userId", "offerTypeCode", "polCode", "podCode", "baseYearWeek")
    val agged2: DataFrame = srcOfer.join(srcLineItem, Seq("offerNumber", "offerChangeSeq"), "left").join(srcRte, Seq("offerNumber", "offerChangeSeq"), "left").groupBy("userId", "offerTypeCode", "polCode").agg(collect_set("BSE_YW").as("baseYearWeek")).withColumn("podCode", lit("all")).select("userId", "offerTypeCode", "polCode", "podCode", "baseYearWeek")
    val agged3: DataFrame = srcOfer.join(srcLineItem, Seq("offerNumber", "offerChangeSeq"), "left").join(srcRte, Seq("offerNumber", "offerChangeSeq"), "left").groupBy("userId", "offerTypeCode", "podCode").agg(collect_set("BSE_YW").as("baseYearWeek")).withColumn("polCode", lit("all")).select("userId", "offerTypeCode", "polCode", "podCode", "baseYearWeek")
    val agged4: DataFrame = srcOfer.join(srcLineItem, Seq("offerNumber", "offerChangeSeq"), "left").join(srcRte, Seq("offerNumber", "offerChangeSeq"), "left").groupBy("userId", "offerTypeCode", "polCode", "podCode").agg(collect_set("BSE_YW").as("baseYearWeek"))
    agged1.printSchema
    agged2.printSchema
    agged3.printSchema
    agged4.printSchema
    val F9S_DSBD_WKLIST = agged1.union(agged2).union(agged3).union(agged4).sort(col("userId").desc)
      .groupBy("userId", "offerTypeCode", "polCode", "podCode", "baseYearWeek")
      .agg(lit("dummy")).distinct.drop("dummy")

//    F9S_DSBD_WKLIST.repartition(1).write.mode("append").json(pathJsonSave + "/F9S_DSBD_WKLIST")

//    F9S_DSBD_WKLIST.write.mode("append").parquet(pathParquetSave + "/F9S_DSBD_WKLIST")

    F9S_DSBD_WKLIST.printSchema
    MongoSpark.save(F9S_DSBD_WKLIST.write
      .option("uri", "mongodb://ec2-13-209-15-68.ap-northeast-2.compute.amazonaws.com:27017/f9s")
      .option("collection", "F9S_DSBD_WKLIST").mode("overwrite"))
    println("/////////////////////////////JOB FINISHED//////////////////////////////")
  }
}
