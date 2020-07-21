package query

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

case class F9S_DSBD_SUM(var spark: SparkSession, var pathSourceFrom: String, var pathParquetSave: String, var pathJsonSave: String, var currentWk: String) {
  def dsbd_sum(): Unit = {
    println("////////////////////////////////DSBD SUM: JOB STARTED////////////////////////////////////////")
    lazy val F9S_DSBD_RAW = spark.read.parquet(pathParquetSave + "/F9S_DSBD_RAW")
    lazy val FTR_DEAL = spark.read.parquet(pathSourceFrom + "/FTR_DEAL")
      .select(
        col("OFER_NR").as("offerNumber"),
        col("OFER_CHNG_SEQ").plus(lit(1)).as("offerChangeSeq"),
        col("DEAL_NR").as("dealNumber"),
        col("DEAL_CHNG_SEQ").as("dealChangeSeq"),
        col("DEAL_DT").as("eventTimestamp")
      )

    lazy val srcRte = F9S_DSBD_RAW
      .select(col("OFER_NR").as("offerNumber"),
        col("OFER_CHNG_SEQ").as("offerChangeSeq"),
        col("polCode"),
        col("podCode"),
        col("polName"),
        col("podName")).distinct
      .groupBy("offerNumber", "offerChangeSeq")
      .agg(countDistinct("polCode").as("polCount"),
        countDistinct("podCode").as("podCount"),
        collect_set(struct("polCode","polName", "podCode","podName")).as("routeItem"))
    lazy val srcLineItem = F9S_DSBD_RAW
      .withColumn("seqChk", row_number().over(Window.partitionBy("OFER_NR", "BSE_YW").orderBy(col("OFER_CHNG_SEQ").desc)))
      .filter(col("seqChk") === 1).drop("seqChk")
      .select(
        col("EMP_NR").as("userId"),
        col("OFER_TP_CD").as("offerTypeCode"),
        col("OFER_NR").as("offerNumber"),
        col("OFER_CHNG_SEQ").as("offerChangeSeq"),
        col("BSE_YW").as("baseYearWeek"),
        col("DEAL_AMT").as("dealAmt"),
        col("DEAL_PRCE").as("dealPrice"),
        col("DEAL_QTY").as("dealQty"),
        col("OFER_PRCE").as("offerPrice"),
        col("OFER_PRCE").multiply(col("OFER_QTY")).as("offerAmt"),
        col("OFER_QTY").as("offerQty"),
        col("OFER_PRCE").as("leftPrice"),
        col("OFER_REMN_QTY").as("leftQty"),
        when(col("OFER_REMN_QTY") =!= 0, col("OFER_REMN_QTY").multiply(col("OFER_PRCE"))).otherwise(lit(0)).as("leftAmt"),
        col("ALL_YN").as("allYn"),
        col("carrierItem"),
        col("carrierCount")
      )
      .join(FTR_DEAL, Seq("offerNumber", "offerChangeSeq"), "left")
      .withColumn("lineEventTimestamp", when(col("eventTimestamp") isNull, col("offerNumber").substr(lit(2), lit(21))).otherwise(col("eventTimestamp")))
      .withColumn("lineReferenceEventNumber", when(col("dealNumber") isNull, col("offerNumber")).otherwise(col("dealNumber")))
      .withColumn("lineReferenceEventChangeSeq", when(col("dealChangeSeq") isNull, col("offerChangeSeq")).otherwise(col("dealChangeSeq")))
      .withColumn("offerLineStatus", when(col("baseYearWeek") > currentWk, lit("1")).otherwise(lit("0")))
      .groupBy("userId",
        "offerTypeCode",
        "offerNumber",
        "offerChangeSeq",
        "allYn",
        "carrierItem",
        "carrierCount"
      )
      .agg(collect_set(struct("baseYearWeek",
        "dealQty",
        "dealPrice",
        "dealAmt",
        "leftQty",
        "leftPrice",
        "leftAmt",
        "lineEventTimestamp",
        "lineReferenceEventNumber",
        "lineReferenceEventChangeSeq")).as("lineItem"),
        max("lineEventTimestamp").as("eventTimestamp"),
        max("lineReferenceEventNumber").as("referenceEventNumber"),
        max("lineReferenceEventChangeSeq").as("referenceEventChangeSeq"),
        max("baseYearWeek").as("maxBaseYearWeek"),
        min("baseYearWeek").as("minBaseYearWeek"),
        sum("leftAmt").as("priceValue"))
      .withColumn("offerStatus",
        when(((col("maxBaseYearWeek") > currentWk and col("minBaseYearWeek") < currentWk) and col("allYn") === 1) or
          (col("minBaseYearWeek") > currentWk) or
          (col("priceValue") > 0)
          , lit("1")).otherwise(lit("0"))).drop("maxBaseYearWeek", "minBaseYearWeek").drop("priceValue")

    val F9S_DSBD_SUM = srcLineItem.join(srcRte, Seq("offerNumber", "offerChangeSeq"), "left")
      .groupBy("userId", "offerTypeCode")
      .agg(collect_set(struct("polCount",
        "podCount",
        "offerNumber",
        "offerChangeSeq",
        "eventTimestamp",
        "referenceEventNumber",
        "referenceEventChangeSeq",
        "allYn",
        "offerStatus",
        "carrierCount",
        "lineItem",
        "routeItem",
        "carrierItem")).as("cell"))

//      F9S_DSBD_SUM.repartition(5).write.mode("append").json(pathJsonSave + "/F9S_DSBD_SUM")
    //    F9S_DSBD_SUM.write.mode("append").parquet(pathParquetSave + "/F9S_DSBD_SUM")
    MongoSpark.save(F9S_DSBD_SUM.write
      .option("uri", "mongodb://ec2-13-209-15-68.ap-northeast-2.compute.amazonaws.com:27017/f9s")
      .option("collection", "F9S_DSBD_SUM").mode("overwrite"))

    F9S_DSBD_SUM.printSchema
    println("/////////////////////////////JOB FINISHED//////////////////////////////")
  }
}
