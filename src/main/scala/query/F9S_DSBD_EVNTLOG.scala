package query
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


case class F9S_DSBD_EVNTLOG(var spark: SparkSession, var pathSourceFrom: String,
                            var pathParquetSave: String, var pathJsonSave: String, var currentWk: String){
  def dsbd_evntlog(): Unit ={
    lazy val FTR_OFER = spark.read.parquet(pathSourceFrom+"/FTR_OFER")
    lazy val FTR_DEAL_RSLT = spark.read.parquet(pathSourceFrom+"/FTR_DEAL_RSLT")
    lazy val FTR_DEAL = spark.read.parquet(pathSourceFrom+"/FTR_DEAL")
    lazy val FTR_OFER_LINE_ITEM = spark.read.parquet(pathSourceFrom+"/FTR_OFER_LINE_ITEM")

    //event01//
    lazy val idxList = FTR_OFER.select(col("OFER_NR").as("offerNumber"), col("OFER_TP_CD").as("offerTypeCode"), col("ALL_YN").as("allYn")).distinct
    lazy val src01a = FTR_OFER.select(col("OFER_NR").as("offerNumber"), col("OFER_DT").as("eventTimestamp"), col("OFER_CHNG_SEQ").as("offerChangeSeq"), col("EMP_NR").as("userId"))
      .filter("")
      .groupBy("offerNumber","eventTimestamp", "userId")
      .agg(min("offerChangeSeq").as("offerChangeSeq"))
    lazy val src01b = FTR_OFER_LINE_ITEM.select(col("OFER_NR").as("offerNumber"), col("OFER_CHNG_SEQ").as("offerChangeSeq"), col("BSE_YW").as("baseYearWeek"), col("OFER_QTY").as("offerQty").cast("double"), col("OFER_PRCE").as("offerPrice").cast("double"))
      .withColumn("offerAmt", (col("offerQty")*col("offerPrice")).cast("double"))
      .withColumn("dealQty", lit(0).cast("double"))
      .withColumn("dealPrice", lit(0).cast("double"))
      .withColumn("dealAmt", lit(0).cast("double"))
      .withColumn("tradeClosing", lit(""))
      .withColumn("leftQty", lit(0).cast("double"))
      .withColumn("leftPrice", lit(0).cast("double"))
      .withColumn("leftAmt", lit(0).cast("double"))
      .withColumn("referenceEventNumber", col("offerNumber"))
      .withColumn("referenceEventNumberChangeSeq", lit(0))

    lazy val agged01 = src01a.join(src01b, Seq("offerNumber", "offerChangeSeq"), "left").distinct.withColumn("eventCode", lit("01")).withColumn("eventName", lit("offerPlaced")).withColumn("lastEventTimestamp", col("eventTimestamp")).withColumn("lastEventHost", col("userId")).groupBy("offerNumber","eventCode", "eventName", "lastEventTimestamp", "lastEventHost").agg(collect_list(struct("eventTimestamp", "referenceEventNumber", "referenceEventNumberChangeSeq", "baseYearWeek", "dealQty", "dealPrice", "dealAmt", "offerQty", "offerPrice", "offerAmt","leftQty", "leftPrice", "leftAmt","tradeClosing")).as("eventLog"))
    lazy val agged01b = agged01.join(idxList, Seq("offerNumber"), "left")
    lazy val event01 = agged01b.select("offerNumber", "offerTypeCode", "allYn", "eventCode", "eventName", "lastEventTimestamp", "lastEventHost", "eventLog")

    //event02
    lazy val src02a = FTR_OFER.select(col("OFER_NR").as("offerNumber"), col("OFER_CHNG_SEQ").as("offerChangeSeq"), col("EMP_NR").as("userId"))
    lazy val src02b = FTR_DEAL.select(col("DEAL_NR").as("dealNumber"), col("DEAL_CHNG_SEQ").as("dealChangeSeq"), col("OFER_NR").as("offerNumber"), col("OFER_CHNG_SEQ").as("offerChangeSeq"), col("DEAL_DT").as("eventTimestamp"))
    lazy val src02c = FTR_DEAL_RSLT.filter(col("DEAL_QTY") > 0).select(col("OFER_NR").as("offerNumber"), col("OFER_CHNG_SEQ").as("offerChangeSeq"), col("DEAL_YW").as("baseYearWeek"), col("DEAL_QTY").as("dealQty").cast("double"), col("DEAL_PRCE").as("dealPrice").cast("double"), col("DEAL_NR").as("dealNumber"), col("DEAL_CHNG_SEQ").as("dealChangeSeq").cast("integer")).withColumn("dealAmt", (col("dealQty")*col("dealPrice")).cast("double")).withColumn("offerQty", lit(0).cast("double")).withColumn("offerPrice", lit(0).cast("double")).withColumn("offerAmt", lit(0).cast("double")).withColumn("leftPrice", lit(0).cast("double")).withColumn("leftQty", lit(0).cast("double")).withColumn("leftAmt", lit(0).cast("double")).withColumn("tradeClosing", lit(""))

    lazy val src02d = src02c.join(src02b, Seq("dealNumber", "dealChangeSeq", "offerNumber", "offerChangeSeq"), "left")
    lazy val src02e = src02a.withColumn("eventCode", lit("02")).withColumn("eventName", lit("dealt"))
    lazy val agged02 = src02d.join(src02e, Seq("offerNumber", "offerChangeSeq"), "left").withColumn("lastEventTimestamp", col("eventTimestamp")).withColumn("referenceEventNumber", col("dealNumber")).withColumn("referenceEventNumberChangeSeq", col("dealChangeSeq")).withColumn("lastEventHost", col("userId")).drop("userId").groupBy("offerNumber", "offerChangeSeq", "dealNumber", "dealChangeSeq", "eventCode", "eventName", "lastEventTimestamp", "lastEventHost").agg(collect_list(struct("eventTimestamp", "referenceEventNumber", "referenceEventNumberChangeSeq","baseYearWeek", "dealQty", "dealPrice", "dealAmt","offerQty", "offerPrice", "offerAmt", "leftQty", "leftPrice", "leftAmt", "tradeClosing")).as("eventLog"))

    lazy val agged02b = agged02.join(idxList, Seq("offerNumber"), "left").drop("dealNumber", "dealChangeSeq", "offerChangeSeq")
    lazy val event02 = agged02b.select("offerNumber", "offerTypeCode", "allYn", "eventCode", "eventName", "lastEventTimestamp", "lastEventHost", "eventLog")

    //event03
    lazy val src03a = FTR_OFER_LINE_ITEM.filter(col("BSE_YW") < currentWk).select(col("OFER_NR").as("offerNumber"), col("OFER_CHNG_SEQ").as("offerChangeSeq"), col("BSE_YW").as("baseYearWeek"), col("OFER_REMN_QTY").as("leftQty")).groupBy("offerNumber", "baseYearWeek", "leftQty").agg(max("offerChangeSeq").as("offerChangeSeq")).withColumn("offerQty", lit(0).cast("double")).withColumn("offerPrice", lit(0).cast("double")).withColumn("offerAmt", lit(0).cast("double")).withColumn("dealQty", lit(0).cast("double")).withColumn("dealPrice", lit(0).cast("double")).withColumn("dealAmt", lit(0).cast("double")).withColumn("leftPrice", lit(0).cast("double")).withColumn("leftAmt", lit(0).cast("double")).withColumn("tradeClosing", lit("")).withColumn("referenceEventNumber", col("offerNumber")).withColumn("referenceEventNumberChangeSeq", col("offerChangeSeq").cast("integer")).withColumn("eventCode", lit("03")).withColumn("eventName", lit("weekExpired")).withColumn("lastEventHost", lit("SYSTEM_ADMIN"))
      .withColumn("year", (col("baseYearWeek").substr(lit(1), lit(4)) cast "bigint")*10000)
      .withColumn("month", (format_number(col("baseYearWeek").substr(lit(5), lit(6))*7/30.4 ,0)+1)*100)
      .withColumn("day", format_number(col("baseYearWeek").substr(lit(5),lit(6))*7%30.4,0))
      .withColumn("eventTimestamp", ((((col("year")+col("month")+col("day")) cast "long") cast "string")+lit("000000000000")).cast("string")).drop("year", "month", "day")
      .withColumn("lastEventTimestamp", col("eventTimestamp").cast("string"))
    lazy val src03b = src03a.groupBy("eventCode", "eventName", "offerNumber", "offerChangeSeq", "lastEventHost").agg(collect_list(struct("eventTimestamp", "referenceEventNumber", "referenceEventNumberChangeSeq", "baseYearWeek", "dealQty", "dealPrice", "dealAmt", "offerQty", "offerPrice", "offerAmt", "leftQty", "leftPrice", "leftAmt", "tradeClosing")).as("eventLog")).drop("offerChangeSeq")
    lazy val src03c = src03a.select("offerNumber", "offerChangeSeq","lastEventTimeStamp").distinct
    lazy val agged03 = src03c.join(src03b, Seq("offerNumber"), "left")
    lazy val agged03b = agged03.join(idxList, Seq("offerNumber"), "left").drop("offerChangeSeq")
    lazy val event03 = agged03b.select("offerNumber", "offerTypeCode", "allYn", "eventCode", "eventName", "lastEventTimestamp", "lastEventHost", "eventLog")


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


    lazy val mwidx = F9S_DSBD_EVNTLOG.select("offerNumber")
    lazy val idx = mwidx.collect()
    lazy val offerNumber = mwidx.select("offerNumber").collect().map(_(0).toString)
    lazy val tgData = F9S_DSBD_EVNTLOG

    for (i <- idx.indices){
      tgData.filter(
        col("offerNumber") === offerNumber(i)
      )
        .write.mode("append").json(pathJsonSave+"/F9S_DSBD_EVNTLOG/"+offerNumber(i))
    }
    F9S_DSBD_EVNTLOG.write.mode("append").parquet(pathParquetSave+"/F9S_DSBD_EVNTLOG")
  }
}
