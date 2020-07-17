package query

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class F9S_DSBD_SUM(var spark: SparkSession, var pathSourceFrom: String, var pathParquetSave: String, var pathJsonSave: String, var currentWk: String){
  def dsbd_sum(): Unit ={
    lazy val FTR_OFER = spark.read.parquet(pathSourceFrom+"/FTR_OFER")
    lazy val FTR_OFER_RTE = spark.read.parquet(pathSourceFrom+"/FTR_OFER_RTE")
    lazy val FTR_OFER_LINE_ITEM = spark.read.parquet(pathSourceFrom+"/FTR_OFER_LINE_ITEM")
    lazy val FTR_DEAL = spark.read.parquet(pathSourceFrom+"/FTR_DEAL")
    lazy val FTR_DEAL_LINE_ITEM = spark.read.parquet(pathSourceFrom+"/FTR_DEAL_LINE_ITEM")
    lazy val FTR_OFER_CRYR = spark.read.parquet(pathSourceFrom+"/FTR_OFER_CRYR")
    lazy val MDM_CRYR = spark.read.parquet(pathSourceFrom+"/MDM_CRYR")


    lazy val srcOfer = FTR_OFER.select(col("EMP_NR").as("userId"), col("OFER_NR").as("offerNumber"), col("OFER_CHNG_SEQ").as("offerChangeSeq"), col("OFER_TP_CD").as("offerTypeCode"), col("ALL_YN").as("allYn")).groupBy("offerNumber","userId", "offerTypeCode", "allYn").agg(max("offerChangeSeq").as("offerChangeSeq"))
    lazy val srcDeal = FTR_DEAL.select(col("OFER_NR").as("offerNumber"), col("OFER_CHNG_SEQ").as("offerChangeSeq"), col("DEAL_NR").as("dealNumber"), col("DEAL_CHNG_SEQ").as("dealChangeSeq"), col("DEAL_DT").as("dealDate"))
    lazy val srcOferLineItem = FTR_OFER_LINE_ITEM.select(col("OFER_NR").as("offerNumber"), col("OFER_CHNG_SEQ").as("offerChangeSeq"), col("BSE_YW").as("baseYearWeek"), col("DEAL_QTY").as("dealQty"), col("OFER_REMN_QTY").as("leftQty"), col("OFER_PRCE"), col("OFER_ITEM_SEQ")).withColumn("priceValue", col("leftQty")*col("OFER_PRCE")).drop("OFER_PRCE").withColumn("remnSts", when(col("leftQty") === 0, lit(0)).otherwise(lit(1))).withColumn("wkSts", when(col("baseYearWeek") < currentWk, lit(0)).otherwise(lit(1))).withColumn("offerLineStatus", when(col("remnSts") === 0 || col("wkSts") === 0, lit("0")).otherwise(lit("1")))
    lazy val srcDealLineItem = FTR_DEAL_LINE_ITEM.select(col("OFER_NR").as("offerNumber"), col("OFER_CHNG_SEQ").as("offerChangeSeq"), col("BSE_YW").as("baseYearWeek"), col("DEAL_NR").as("dealNumber"), col("DEAL_CHNG_SEQ").as("dealChangeSeq"))
    lazy val srcPolRte = FTR_OFER_RTE.filter(col("TRDE_LOC_TP_CD")==="02").select(col("TRDE_LOC_CD"), col("OFER_CHNG_SEQ").as("offerChangeSeq"), col("OFER_NR").as("offerNumber"), col("OFER_REG_SEQ")).withColumn("polCode", col("TRDE_LOC_CD")).drop("TRDE_LOC_CD")
    lazy val srcPodRte = FTR_OFER_RTE.filter(col("TRDE_LOC_TP_CD")==="03").select(col("TRDE_LOC_CD"), col("OFER_CHNG_SEQ").as("offerChangeSeq"), col("OFER_NR").as("offerNumber"), col("OFER_REG_SEQ")).withColumn("podCode", col("TRDE_LOC_CD")).drop("TRDE_LOC_CD")
    lazy val srcRte = srcPolRte.join(srcPodRte, Seq("offerNumber", "offerChangeSeq", "OFER_REG_SEQ"), "left").drop("OFER_REG_SEQ")
    lazy val counts = srcRte.groupBy("offerNumber", "offerChangeSeq").agg(countDistinct("polCode").as("polCount"), countDistinct("podCode").as("podCount"))

    lazy val srcOferCryr = FTR_OFER_CRYR.select(col("OFER_NR").as("offerNumber"), col("OFER_CHNG_SEQ").as("offerChangeSeq"), col("OFER_CRYR_CD").as("carrierCode"))
    lazy val mdmCryr = MDM_CRYR.select(col("CRYR_CD").as("carrierCode"), col("CRYR_NM").as("carrierName"))
    lazy val srcOferCryr2 = srcOferCryr.join(mdmCryr, Seq("carrierCode"), "left")
    lazy val countCryr = srcOferCryr2.groupBy("offerNumber", "offerChangeSeq").agg(countDistinct("carrierCode").as("carrierCount"), collect_list(struct("carrierCode", "carrierName")).as("carrierItem"))

    lazy val agg1 = srcDealLineItem.join(srcDeal, Seq("offerNumber", "offerChangeSeq", "dealNumber", "dealChangeSeq"),"left").distinct
    lazy val agg2 = srcOferLineItem.join(agg1, Seq("offerNumber", "offerChangeSeq", "baseYearWeek"),"left").distinct
    lazy val agg3 = srcRte.join(agg2, Seq("offerNumber", "offerChangeSeq"), "left").distinct
    lazy val agg4 = agg3.join(counts, Seq("offerNumber", "offerChangeSeq"), "left").distinct
    lazy val agg5 = agg4.join(countCryr, Seq("offerNumber", "offerChangeSeq"), "left").distinct
    lazy val agg6 = srcOfer.join(agg5, Seq("offerNumber", "offerChangeSeq"),"left").distinct

    lazy val srcAgged = agg6.withColumn("lineEventTimestamp", when(col("dealDate").isNull, col("offerNumber").substr(lit(2), lit(21))).otherwise(col("dealDate")))
      .withColumn("lineReferenceEventNumber", when(col("dealNumber").isNull, col("offerNumber")).otherwise(col("dealNumber")))
      .withColumn("lineReferenceEventChangeSeq", when(col("dealChangeSeq").isNull, col("offerChangeSeq")).otherwise(col("dealChangeSeq")))
      .groupBy("userId", "offerTypeCode","offerNumber", "offerChangeSeq","allYn", "polCount", "podCount", "carrierCount", "carrierItem")
      .agg(collect_set(struct("polCode", "podCode")).as("routeItem"), collect_set(struct("baseYearWeek", "dealQty", "leftQty", "priceValue", "lineEventTimestamp", "lineReferenceEventNumber", "lineReferenceEventChangeSeq","offerLineStatus")).as("lineItem"), max("lineEventTimestamp").as("eventTimestamp"), max("lineReferenceEventNumber").as("referenceEventNumber"), max("lineReferenceEventChangeSeq").as("referenceEventChangeSeq"), sum("remnSts").as("remnSts"), sum("wkSts").as("wkSts"))
      .withColumn("offerStatus", when((col("remnSts") === 0) ||(col("wkSts") === 0) || (col("wkSts") < lit("OFER_ITEM_SEQ") and col("allYn") === "1"), lit("0")).otherwise(lit("1")))
      .drop("OFER_ITEM_SEQ", "remnSts", "wkSts")

    val F9S_DSBD_SUM = srcAgged.groupBy("userId", "offerTypeCode").agg(collect_set(struct("offerNumber", "offerChangeSeq", "allYn", "routeItem", "lineItem", "eventTimestamp", "referenceEventNumber", "referenceEventChangeSeq", "polCount", "podCount", "offerStatus", "carrierItem", "carrierCount")).as("cell"))

    F9S_DSBD_SUM.printSchema
    lazy val mwidx = F9S_DSBD_SUM.select("userId", "offerTypeCode")
    lazy val idx = mwidx.collect()
    lazy val userId = mwidx.select("userId").collect().map(_(0).toString)
    lazy val offerTypeCode = mwidx.select("offerTypeCode").collect().map(_(0).toString)

    lazy val tgData = F9S_DSBD_SUM

    for (i <- idx.indices){
      tgData.filter(
        col("userId") === userId(i) &&
          col("offerTypeCode") === offerTypeCode(i)
      )
        .write.mode("append").json(pathJsonSave+"/F9S_DSBD_SUM"+"/"+userId(i)+"/"+offerTypeCode(i))
    }

    F9S_DSBD_SUM.write.mode("append").parquet(pathParquetSave+"/F9S_DSBD_SUM")
  }
}
