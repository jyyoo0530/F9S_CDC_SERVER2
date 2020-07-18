package query
import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


case class F9S_DSBD_EVNTLOG(var spark: SparkSession, var pathSourceFrom: String,
                            var pathParquetSave: String, var pathJsonSave: String, var currentWk: String){
  def dsbd_evntlog(): Unit ={
    println("////////////////////////////////DSBD EVENTLOG: JOB STARTED////////////////////////////////////////")
    lazy val FTR_OFER = spark.read.parquet(pathSourceFrom+"/FTR_OFER")
//    lazy val FTR_DEAL_RSLT = spark.read.parquet(pathSourceFrom+"/FTR_DEAL_RSLT")
    lazy val FTR_DEAL = spark.read.parquet(pathSourceFrom+"/FTR_DEAL")
    lazy val FTR_OFER_LINE_ITEM = spark.read.parquet(pathSourceFrom+"/FTR_OFER_LINE_ITEM")
      .join(FTR_DEAL.select("DEAL_NR", "DEAL_CHNG_SEQ", "DEAL_DT", "OFER_NR", "OFER_CHNG_SEQ"),
        Seq("OFER_NR", "OFER_CHNG_SEQ"), "left")
      .join(FTR_OFER.select("OFER_NR", "OFER_CHNG_SEQ", "OFER_DT", "OFER_TP_CD", "ALL_YN"),
        Seq("OFER_NR", "OFER_CHNG_SEQ"), "left")

    //event01//
    lazy val src01 = FTR_OFER_LINE_ITEM.filter(col("OFER_CHNG_SEQ") === 0)
      .select(col("OFER_NR").as("offerNumber"),
        col("OFER_TP_CD").as("offerTypeCode"),
        col("ALL_YN").as("allYn")
      )
      .withColumn("eventCode", lit("01"))
      .withColumn("eventName", lit("offerPlaced"))
      .withColumn("lastEventTimestamp", col("OFER_NR").substr(lit(2),lit(22)))
      .withColumn("lastEventHost", lit("F9_SYSTETM_ADMIN"))
      .withColumn("eventTimestamp", col("OFER_NR").substr(lit(2), lit(22)))
      .withColumn("referenceEventNumber", col("OFER_NR"))
      .withColumn("referenceEventNumberChangeSeq", col("OFER_CHNG_SEQ"))
      .select(
        col("BSE_YW").as("baseYearWeek"),
        col("DEAL_PRCE").as("dealPrice"),
        col("DEAL_QTY").as("dealQty"),
        col("DEAL_AMT").as("dealAmt"),
        col("OFER_PRCE").as("offerPrice"),
        col("OFER_QTY").as("offerQty"),
        col("OFER_PRCE")*col("OFER_QTY").as("offerAmt"),
        col("OFER_PRCE").as("leftPrice"),
        col("OFER_REMN_QTY").as("leftQty"),
        col("OFER_PRCE")*col("OFER_REMN_QTY").as("leftAmt")
      )
      .withColumn("tradeClosing", lit(""))
    lazy val event01 = src01.groupBy("offerNumber","offerTypeCode", "allYn", "eventCode","eventName","lastEventTimeStamp","lastEventHost")
      .agg(collect_set(struct("eventTimestamp",
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
      .groupBy("offerNumber").agg(collect_set(struct("eventCode",
      "eventName",
      "lastEventTimestamp",
      "lastEventHost",
      "eventLog")).as("eventCell"))

    //event02
    lazy val src02 = FTR_OFER_LINE_ITEM.filter(col("OFER_CHNG_SEQ") > 0)
      .select(col("OFER_NR").as("offerNumber"),
        col("OFER_TP_CD").as("offerTypeCode"),
        col("ALL_YN").as("allYn"),
        col("OFER_CHNG_SEQ").as("offerChangeSeq")
      )
      .withColumn("eventCode", lit("02"))
      .withColumn("eventName", lit("offerDealt"))
      .withColumn("lastEventTimestamp", col("DEAL_DT"))
      .withColumn("lastEventHost", lit("F9_SYSTETM_ADMIN"))
      .withColumn("eventTimestamp", col("DEAL_DT"))
      .withColumn("referenceEventNumber", col("DEAL_NR"))
      .withColumn("referenceEventNumberChangeSeq", col("DEAL_CHNG_SEQ"))
      .select(
        col("BSE_YW").as("baseYearWeek"),
        col("DEAL_PRCE").as("dealPrice"),
        col("DEAL_QTY").as("dealQty"),
        col("DEAL_AMT").as("dealAmt"),
        col("OFER_PRCE").as("offerPrice"),
        col("OFER_QTY").as("offerQty"),
        col("OFER_PRCE")*col("OFER_QTY").as("offerAmt"),
        col("OFER_PRCE").as("leftPrice"),
        col("OFER_REMN_QTY").as("leftQty"),
        col("OFER_PRCE")*col("OFER_REMN_QTY").as("leftAmt"))
      .withColumn("tradeClosing", lit(""))
    lazy val event02 = src02.groupBy("offerNumber", "offerChangeSeq","eventIdx", "eventCode", "eventName", "lastEventTimeStamp", "lastEventHost")
      .agg(collect_set(struct("eventTimestamp",
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
        "tradeClosing")).as("eventLog")).drop("offerChangeSeq")
      .groupBy("offerNumber").agg(collect_set(struct("eventCode",
      "eventName",
      "lastEventTimestamp",
      "lastEventHost",
      "eventLog")).as("eventCell"))

    //event03
    lazy val src03a = FTR_OFER_LINE_ITEM.filter(col("BSE_YW") < currentWk)
      .select(col("OFER_NR").as("offerNumber"),
        col("OFER_TP_CD").as("offerTypeCode"),
        col("ALL_YN").as("allYn"),
        col("OFER_CHNG_SEQ").as("offerChangeSeq")
      )
      .withColumn("eventCode", lit("03"))
      .withColumn("eventName", lit("weekExpired"))
      .withColumn("lastEventTimestamp", col("DEAL_DT"))
      .withColumn("lastEventHost", lit("F9_SYSTETM_ADMIN"))
      .withColumn("eventTimestamp", col("DEAL_DT"))
      .withColumn("referenceEventNumber", col("DEAL_NR"))
      .withColumn("referenceEventNumberChangeSeq", col("DEAL_CHNG_SEQ"))
      .select(
        col("BSE_YW").as("baseYearWeek"),
        col("DEAL_PRCE").as("dealPrice"),
        col("DEAL_QTY").as("dealQty"),
        col("DEAL_AMT").as("dealAmt"),
        col("OFER_PRCE").as("offerPrice"),
        col("OFER_QTY").as("offerQty"),
        col("OFER_PRCE")*col("OFER_QTY").as("offerAmt"),
        col("OFER_PRCE").as("leftPrice"),
        col("OFER_REMN_QTY").as("leftQty"),
        col("OFER_PRCE")*col("OFER_REMN_QTY").as("leftAmt"))
      .withColumn("tradeClosing", lit(""))
    lazy val event02 = src02.groupBy("offerNumber", "offerChangeSeq","eventIdx", "eventCode", "eventName", "lastEventTimeStamp", "lastEventHost")
      .agg(collect_set(struct("eventTimestamp",
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
        "tradeClosing")).as("eventLog")).drop("offerChangeSeq")
      .groupBy("offerNumber").agg(collect_set(struct("eventCode",
      "eventName",
      "lastEventTimestamp",
      "lastEventHost",
      "eventLog")).as("eventCell"))




    //event04
    lazy val src04a = FTR_DEAL.select(col("OFER_NR").as("offerNumber"), col("OFER_CHNG_SEQ").as("offerChangeSeq"), col("DEAL_DT").as("eventTimestamp"), col("DEAL_NR").as("referenceEventNumber"), col("DEAL_CHNG_SEQ").as("referenceEventNumberChangeSeq").cast("integer"))
    lazy val src04b = FTR_OFER_LINE_ITEM.select(col("OFER_NR").as("offerNumber"), col("OFER_CHNG_SEQ").as("offerChangeSeq"), col("OFER_REMN_QTY").as("leftQty")).groupBy("offerNumber").agg(max("offerChangeSeq").as("offerChangeSeq"), sum("leftQty").as("leftQty"))
    lazy val src04c = src04b.join(src04a, Seq("offerNumber", "offerChangeSeq"), "left").filter(col("leftQty") === 0).withColumn("eventCode", lit("04")).withColumn("eventName", lit("allDealt")).withColumn("lastEventTimestamp", col("eventTimestamp")).withColumn("lastEventHost", lit("SYSTEM_ADMIN")).withColumn("baseYearWeek", lit("")).withColumn("dealQty", lit(0).cast("double")).withColumn("dealPrice", lit(0).cast("double")).withColumn("dealAmt", lit(0).cast("double")).withColumn("offerQty", lit(0).cast("double")).withColumn("offerPrice", lit(0).cast("double")).withColumn("offerAmt", lit(0).cast("double")).withColumn("leftQty", lit(0).cast("double")).withColumn("leftPrice", lit(0).cast("double")).withColumn("leftAmt", lit(0).cast("double")).withColumn("tradeClosing", lit(""))
    lazy val agged04 = src04c.groupBy("offerNumber", "eventCode", "eventName", "lastEventTimeStamp", "lastEventHost").agg(collect_set(struct("eventTimestamp", "referenceEventNumber", "referenceEventNumberChangeSeq", "baseYearWeek", "dealQty", "dealPrice", "dealAmt", "offerQty", "offerPrice", "offerAmt", "leftQty", "leftPrice", "leftAmt", "tradeClosing")).as("eventLog"))
    lazy val agged04b = agged04.join(idxList, Seq("offerNumber"), "left")
    lazy val event04 = agged04b.select("offerNumber", "offerTypeCode", "allYn", "eventCode", "eventName", "lastEventTimestamp", "lastEventHost", "eventLog")

    //event04
    lazy val idxList2 = FTR_OFER.select(col("OFER_NR").as("offerNumber"), col("ALL_YN").as("allYn")).distinct
    lazy val src05a = FTR_DEAL.select(col("OFER_NR").as("offerNumber"), col("OFER_CHNG_SEQ").as("offerChangeSeq"), col("DEAL_DT").as("eventTimestamp"), col("DEAL_NR").as("referenceEventNumber"), col("DEAL_CHNG_SEQ").as("referenceEventNumberChangeSeq").cast("integer"))
    lazy val src05b = FTR_OFER_LINE_ITEM.select(col("OFER_NR").as("offerNumber"), col("OFER_CHNG_SEQ").as("offerChangeSeq"), col("OFER_REMN_QTY").as("leftQty"), col("BSE_YW").as("baseYearWeek")).join(idxList2, Seq("offerNumber"), "left")
    lazy val src05c = src05b.withColumn("wkSts", when(col("baseYearWeek") < currentWk, lit(0)).otherwise(lit(1))).groupBy("offerNumber", "allYn").agg(max("offerChangeSeq").as("offerChangeSeq"), sum("leftQty").as("leftQty"), sum("wkSts").as("allWkPass"), max("wkSts").as("partialWkPass")).filter(col("leftQty") === 0 || (col("allWkPass") === 0 and col("allYn") === 0) || (col("partialWkPass") === 0 and col("allYn") === 1))
    lazy val src05d = src05c.join(src05a, Seq("offerNumber", "offerChangeSeq"), "left").withColumn("eventCode", lit("05")).withColumn("eventName", lit("offerClosed")).withColumn("lastEventTimestamp", col("eventTimestamp")).withColumn("lastEventHost", lit("SYSTEM_ADMIN")).withColumn("baseYearWeek", lit("")).withColumn("dealQty", lit(0).cast("double")).withColumn("dealPrice", lit(0).cast("double")).withColumn("dealAmt", lit(0).cast("double")).withColumn("offerQty", lit(0).cast("double")).withColumn("offerPrice", lit(0).cast("double")).withColumn("offerAmt", lit(0).cast("double")).withColumn("leftQty", lit(0).cast("double")).withColumn("leftPrice", lit(0).cast("double")).withColumn("leftAmt", lit(0).cast("double")).withColumn("tradeClosing", lit(""))
    lazy val agged05 = src05d.groupBy("offerNumber", "eventCode", "eventName", "lastEventTimeStamp", "lastEventHost").agg(collect_set(struct("eventTimestamp", "referenceEventNumber", "referenceEventNumberChangeSeq", "baseYearWeek", "dealQty", "dealPrice", "dealAmt", "offerQty", "offerPrice", "offerAmt", "leftQty", "leftPrice", "leftAmt", "tradeClosing")).as("eventLog"))
    lazy val agged05b = agged05.join(idxList, Seq("offerNumber"), "left")
    lazy val event05 = agged05b.select("offerNumber", "offerTypeCode", "allYn", "eventCode", "eventName", "lastEventTimestamp", "lastEventHost", "eventLog")


    val F9S_DSBD_EVNTLOG =  event01.union(event02).union(event03).union(event04).union(event05).groupBy("offerNumber", "offerTypeCode", "allYn").agg(collect_list(struct("eventCode", "eventName", "lastEventTimestamp", "lastEventHost", "eventLog")).as("eventCell"))


//    F9S_DSBD_EVNTLOG.repartition(1).write.mode("append").json(pathJsonSave+"/F9S_DSBD_EVNTLOG")
//    F9S_DSBD_EVNTLOG.write.mode("append").parquet(pathParquetSave+"/F9S_DSBD_EVNTLOG")

    MongoSpark.save(F9S_DSBD_EVNTLOG.write
      .option("uri", "mongodb://data.freight9.com/f9s")
      .option("collection", "F9S_DSBD_EVNTLOG").mode("overwrite"))
    F9S_DSBD_EVNTLOG.printSchema
    println("/////////////////////////////JOB FINISHED//////////////////////////////")
  }
}
