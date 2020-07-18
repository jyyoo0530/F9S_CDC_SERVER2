package query

import com.mongodb.spark.MongoSpark
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StructField, StructType}


case class F9S_DSBD_EVNTLOG(var spark: SparkSession, var pathSourceFrom: String,
                            var pathParquetSave: String, var pathJsonSave: String, var currentWk: String) {
  def dsbd_evntlog(): Unit = {
    println("////////////////////////////////DSBD EVENTLOG: JOB STARTED////////////////////////////////////////")
    lazy val FTR_OFER = spark.read.parquet(pathSourceFrom + "/FTR_OFER")
    //    lazy val FTR_DEAL_RSLT = spark.read.parquet(pathSourceFrom+"/FTR_DEAL_RSLT")
    lazy val FTR_DEAL = spark.read.parquet(pathSourceFrom + "/FTR_DEAL")
    lazy val weektable = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(pathSourceFrom + "/weektable.csv")
      .select(col("BSE_YW").cast("String"), col("yyyymmdd").cast("String")).withColumn("timestamp", concat(col("yyyymmdd"), lit("010000000000"))).drop("yyyymmdd")

    lazy val FTR_OFER_LINE_ITEM = spark.read.parquet(pathSourceFrom + "/FTR_OFER_LINE_ITEM")
      .join(FTR_DEAL.select("DEAL_NR", "DEAL_CHNG_SEQ", "DEAL_DT", "OFER_NR", "OFER_CHNG_SEQ"),
        Seq("OFER_NR", "OFER_CHNG_SEQ"), "left")
      .join(FTR_OFER.select("OFER_NR", "OFER_CHNG_SEQ", "OFER_DT", "OFER_TP_CD", "ALL_YN"),
        Seq("OFER_NR", "OFER_CHNG_SEQ"), "left")
      .join(weektable,
        Seq("BSE_YW"), "left")
    lazy val idx = FTR_OFER.select(col("OFER_NR").as("offerNumber"), col("ALL_YN").as("allYn"), col("OFER_TP_CD").as("offerTypeCode"))

    //event01//
    lazy val src01 = FTR_OFER_LINE_ITEM.filter(col("OFER_CHNG_SEQ") === 0)
      .select(col("OFER_NR").as("offerNumber"),
        col("OFER_TP_CD").as("offerTypeCode"),
        col("ALL_YN").as("allYn"),
        col("OFER_CHNG_SEQ").as("offerChangeSeq"),
        col("BSE_YW").as("baseYearWeek"),
        col("DEAL_PRCE").as("dealPrice"),
        col("DEAL_QTY").as("dealQty"),
        col("DEAL_AMT").as("dealAmt"),
        col("OFER_PRCE").as("offerPrice"),
        col("OFER_QTY").as("offerQty"),
        col("OFER_PRCE").multiply(col("OFER_QTY")).as("offerAmt"),
        col("OFER_PRCE").as("leftPrice"),
        col("OFER_REMN_QTY").as("leftQty"),
        col("OFER_PRCE").multiply(col("OFER_REMN_QTY")).as("leftAmt"),
        col("timestamp")
      )
      .withColumn("eventCode", lit("01"))
      .withColumn("eventName", lit("offerPlaced"))
      .withColumn("lastEventTimestamp", col("offerNumber").substr(lit(2), lit(21)))
      .withColumn("lastEventHost", lit("F9_SYSTETM_ADMIN"))
      .withColumn("eventTimestamp", col("offerNumber").substr(lit(2), lit(21)))
      .withColumn("referenceEventNumber", col("offerNumber"))
      .withColumn("referenceEventNumberChangeSeq", col("offerChangeSeq"))
      .withColumn("tradeClosing", col("timestamp"))
    lazy val event01 = src01
      .withColumn("lastEventTimestamp", lit(max("eventTimestamp").over(Window.partitionBy("offerNumber"))))
      .groupBy("offerNumber", "offerTypeCode", "allYn", "offerChangeSeq", "eventCode", "eventName", "lastEventTimestamp", "lastEventHost")
      .agg(
        collect_set(struct("eventTimestamp",
          "referenceEventNumber",
          "referenceEventNumberChangeSeq",
          "baseYearWeek",
          "dealPrice",
          "dealQty",
          "dealAmt",
          "offerPrice",
          "offerQty",
          "offerAmt",
          "leftPrice",
          "leftQty",
          "leftAmt",
          "tradeClosing")).as("eventLog")).drop("offerChangeSeq", "timestamp")
    event01.printSchema

    //event02
    lazy val src02 = FTR_OFER_LINE_ITEM.filter(col("OFER_CHNG_SEQ") > 0)
      .select(col("OFER_NR").as("offerNumber"),
        col("OFER_TP_CD").as("offerTypeCode"),
        col("ALL_YN").as("allYn"),
        col("OFER_CHNG_SEQ").as("offerChangeSeq"),
        col("BSE_YW").as("baseYearWeek"),
        col("DEAL_PRCE").as("dealPrice"),
        col("DEAL_QTY").as("dealQty"),
        col("DEAL_AMT").as("dealAmt"),
        col("OFER_PRCE").as("offerPrice"),
        col("OFER_QTY").as("offerQty"),
        col("OFER_PRCE").multiply(col("OFER_QTY")).as("offerAmt"),
        col("OFER_PRCE").as("leftPrice"),
        col("OFER_REMN_QTY").as("leftQty"),
        col("OFER_PRCE").multiply(col("OFER_REMN_QTY")).as("leftAmt"),
        col("DEAL_DT"),
        col("DEAL_NR"),
        col("DEAL_CHNG_SEQ"),
        col("timestamp")
      )
      .withColumn("eventCode", lit("02"))
      .withColumn("eventName", lit("offerDealt"))
      .withColumn("lastEventTimestamp", col("DEAL_DT"))
      .withColumn("lastEventHost", lit("F9_SYSTETM_ADMIN"))
      .withColumn("eventTimestamp", col("DEAL_DT"))
      .withColumn("referenceEventNumber", col("DEAL_NR"))
      .withColumn("referenceEventNumberChangeSeq", col("DEAL_CHNG_SEQ"))
      .withColumn("tradeClosing", col("timestamp"))
    lazy val event02 = src02
      .withColumn("lastEventTimestamp", lit(max("eventTimestamp").over(Window.partitionBy("offerNumber", "offerChangeSeq"))))
      .filter(col("referenceEventNumber") isNotNull)
      .groupBy("offerNumber", "offerTypeCode", "allYn", "offerChangeSeq", "eventCode", "eventName", "lastEventTimestamp", "lastEventHost")
      .agg(
        collect_set(struct("eventTimestamp",
          "referenceEventNumber",
          "referenceEventNumberChangeSeq",
          "baseYearWeek",
          "dealPrice",
          "dealQty",
          "dealAmt",
          "offerPrice",
          "offerQty",
          "offerAmt",
          "leftPrice",
          "leftQty",
          "leftAmt",
          "tradeClosing")).as("eventLog")).drop("offerChangeSeq", "DEAL_DT", "DEAL_NR", "DEAL_CHNG_SEQ", "timestamp")
    event02.printSchema

    //event03
    lazy val src03 = FTR_OFER_LINE_ITEM.filter(col("BSE_YW") < currentWk)
      .select(col("OFER_NR").as("offerNumber"),
        col("OFER_TP_CD").as("offerTypeCode"),
        col("ALL_YN").as("allYn"),
        col("OFER_CHNG_SEQ").as("offerChangeSeq"),
        col("BSE_YW").as("baseYearWeek"),
        col("DEAL_PRCE").as("dealPrice"),
        col("DEAL_QTY").as("dealQty"),
        col("DEAL_AMT").as("dealAmt"),
        col("OFER_PRCE").as("offerPrice"),
        col("OFER_QTY").as("offerQty"),
        col("OFER_PRCE").multiply(col("OFER_QTY")).as("offerAmt"),
        col("OFER_PRCE").as("leftPrice"),
        col("OFER_REMN_QTY").as("leftQty"),
        col("OFER_PRCE").multiply(col("OFER_REMN_QTY")).as("leftAmt"),
        col("timestamp")
      )
      .withColumn("eventCode", lit("03"))
      .withColumn("eventName", lit("weekExpired"))
      .withColumn("lastEventTimestamp", col("timestamp"))
      .withColumn("lastEventHost", lit("F9_SYSTETM_ADMIN"))
      .withColumn("eventTimestamp", col("timestamp"))
      .withColumn("referenceEventNumber", col("offerNumber"))
      .withColumn("referenceEventNumberChangeSeq", col("offerChangeSeq"))
      .withColumn("tradeClosing", col("timestamp"))
    lazy val event03 = src03
      .withColumn("lastEventTimestamp", lit(max("eventTimestamp").over(Window.partitionBy("offerNumber"))))
      .groupBy("offerNumber", "offerTypeCode", "allYn", "offerChangeSeq", "eventCode", "eventName", "lastEventTimestamp", "lastEventHost")
      .agg(
        max("offerChangeSeq").over(Window.partitionBy("offerNumber")).as("maxOfferChangeSeq"),
        collect_set(struct("eventTimestamp",
          "referenceEventNumber",
          "referenceEventNumberChangeSeq",
          "baseYearWeek",
          "dealPrice",
          "dealQty",
          "dealAmt",
          "offerPrice",
          "offerQty",
          "offerAmt",
          "leftPrice",
          "leftQty",
          "leftAmt",
          "tradeClosing")).as("eventLog"))
      .filter(col("offerChangeSeq") === col("maxOfferChangeSeq"))
      .drop("offerChangeSeq", "maxOfferChangeSeq", "timestamp")
    event03.printSchema

    //event04
    lazy val src04 = FTR_OFER_LINE_ITEM
      .select(col("OFER_NR").as("offerNumber"),
        col("OFER_TP_CD").as("offerTypeCode"),
        col("ALL_YN").as("allYn"),
        col("OFER_CHNG_SEQ").as("offerChangeSeq"),
        col("BSE_YW").as("baseYearWeek"),
        col("DEAL_PRCE").as("dealPrice"),
        col("DEAL_QTY").as("dealQty"),
        col("DEAL_AMT").as("dealAmt"),
        col("OFER_PRCE").as("offerPrice"),
        col("OFER_QTY").as("offerQty"),
        col("OFER_PRCE").multiply(col("OFER_QTY")).as("offerAmt"),
        col("OFER_PRCE").as("leftPrice"),
        col("OFER_REMN_QTY").as("leftQty"),
        col("OFER_PRCE").multiply(col("OFER_REMN_QTY")).as("leftAmt"),
        col("DEAL_DT"),
        col("timestamp")
      )
      .withColumn("eventCode", lit("04"))
      .withColumn("eventName", lit("allDealt"))
      .withColumn("lastEventTimestamp", col("DEAL_DT"))
      .withColumn("lastEventHost", lit("F9_SYSTETM_ADMIN"))
      .withColumn("eventTimestamp", col("DEAL_DT"))
      .withColumn("referenceEventNumber", col("offerNumber"))
      .withColumn("referenceEventNumberChangeSeq", col("offerChangeSeq"))
      .withColumn("tradeClosing", col("timestamp"))
    lazy val event04 = src04
      .withColumn("lastEventTimestamp", lit(max("eventTimestamp").over(Window.partitionBy("offerNumber"))))
      .groupBy("offerNumber", "offerTypeCode", "allYn", "offerChangeSeq", "eventCode", "eventName", "lastEventTimestamp", "lastEventHost")
      .agg(sum("leftQty").as("allDealt"),
        collect_set(struct("eventTimestamp",
          "referenceEventNumber",
          "referenceEventNumberChangeSeq",
          "baseYearWeek",
          "dealPrice",
          "dealQty",
          "dealAmt",
          "offerPrice",
          "offerQty",
          "offerAmt",
          "leftPrice",
          "leftQty",
          "leftAmt",
          "tradeClosing")).as("eventLog"))
      .filter(col("allDealt") === 0).drop("offerChangeSeq", "allDealt", "DEAL_DT", "timestamp")
    event04.printSchema

    //event05
    lazy val src05 = FTR_OFER_LINE_ITEM
      .select(col("OFER_NR").as("offerNumber"),
        col("OFER_TP_CD").as("offerTypeCode"),
        col("ALL_YN").as("allYn"),
        col("OFER_CHNG_SEQ").as("offerChangeSeq"),
        col("BSE_YW").as("baseYearWeek"),
        col("DEAL_PRCE").as("dealPrice"),
        col("DEAL_QTY").as("dealQty"),
        col("DEAL_AMT").as("dealAmt"),
        col("OFER_PRCE").as("offerPrice"),
        col("OFER_QTY").as("offerQty"),
        col("OFER_PRCE").multiply(col("OFER_QTY")).as("offerAmt"),
        col("OFER_PRCE").as("leftPrice"),
        col("OFER_REMN_QTY").as("leftQty"),
        col("OFER_PRCE").multiply(col("OFER_REMN_QTY")).as("leftAmt"),
        col("DEAL_DT"),
        col("timestamp")
      )
      .withColumn("eventCode", lit("05"))
      .withColumn("eventName", lit("offerClosed"))
      .withColumn("lastEventTimestamp", when(col("DEAL_DT") > col("timestamp"), col("DEAL_DT")).otherwise(col("timestamp")))
      .withColumn("lastEventHost", lit("F9_SYSTETM_ADMIN"))
      .withColumn("eventTimestamp", col("DEAL_DT"))
      .withColumn("referenceEventNumber", col("offerNumber"))
      .withColumn("referenceEventNumberChangeSeq", col("offerChangeSeq"))
      .withColumn("tradeClosing", when(col("DEAL_DT") > col("timestamp"), col("DEAL_DT")).otherwise(col("timestamp"))).distinct
    lazy val event05 = src05
      .withColumn("lastEventTimestamp", lit(max("eventTimestamp").over(Window.partitionBy("offerNumber"))))
      .groupBy("offerNumber", "offerTypeCode", "allYn", "offerChangeSeq", "eventCode", "eventName", "lastEventTimestamp", "lastEventHost")
      .agg(sum("leftQty").as("allDealt"),
        max("baseYearWeek").as("maxWeek"),
        min("baseYearWeek").as("minWeek"),
        max("offerChangeSeq").over(Window.partitionBy("offerNumber")).as("maxOfferChangeSeq"),
        collect_set(struct("eventTimestamp",
          "referenceEventNumber",
          "referenceEventNumberChangeSeq",
          "baseYearWeek",
          "dealPrice",
          "dealQty",
          "dealAmt",
          "offerPrice",
          "offerQty",
          "offerAmt",
          "leftPrice",
          "leftQty",
          "leftAmt",
          "tradeClosing")).as("eventLog"))
      .filter(
        col("offerChangeSeq") === col("maxOfferChangeSeq")
      )
      .filter(
        col("allDealt") === 0
          or (col("allYn") === 1 and col("minWeek") > currentWk and col("maxWeek") < currentWk)
          or (col("maxWeek") < currentWk)
      )
      .drop("offerChangeSeq", "allDealt", "minWeek", "maxWeek", "DEAL_DT", "timestamp", "maxOfferChangeSeq")
    event05.printSchema


    val F9S_DSBD_EVNTLOG = event01.union(event02).union(event03).union(event04).union(event05)
      .groupBy("offerNumber", "allYn", "offerTypeCode").agg(collect_set(struct("eventCode",
      "eventName",
      "lastEventTimestamp",
      "lastEventHost",
      "eventLog")).as("eventCell"))
    //    val F9S_DSBD_EVNTLOG = event03
    //      .groupBy("offerNumber", "allYn", "offerTypeCode").agg(collect_set(struct("eventCode",
    //      "eventName",
    //      "lastEventTimestamp",
    //      "lastEventHost",
    //      "eventLog")).as("eventCell"))

    F9S_DSBD_EVNTLOG.printSchema

    //    F9S_DSBD_EVNTLOG.write.mode("append").json(pathJsonSave+"/F9S_DSBD_EVNTLOG")
    //    F9S_DSBD_EVNTLOG.write.mode("append").parquet(pathParquetSave+"/F9S_DSBD_EVNTLOG")

    MongoSpark.save(F9S_DSBD_EVNTLOG.write
      .option("uri", "mongodb://data.freight9.com/f9s")
      .option("collection", "F9S_DSBD_EVNTLOG").mode("overwrite"))

    println("/////////////////////////////JOB FINISHED//////////////////////////////")
  }
}
