package query

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class F9S_DSBD_WKDETAIL(var spark: SparkSession, var pathSourceFrom: String, var pathParquetSave: String, var pathJsonSave: String) {
  def dsbd_wkdetail(): Unit = {
    lazy val FTR_DEAL_LINE_ITEM = spark.read.parquet(pathSourceFrom + "/FTR_DEAL_LINE_ITEM")
    lazy val FTR_DEAL = spark.read.parquet(pathSourceFrom + "/FTR_DEAL")

    lazy val srcDeal = FTR_DEAL.select(col("DEAL_DT").as("eventTimestamp"), col("DEAL_NR").as("referenceEventNumber"), col("DEAL_CHNG_SEQ").as("referenceEventChangeSeq"))
    lazy val srcDealLineItem = FTR_DEAL_LINE_ITEM.select(col("OFER_NR").as("offerNumber"), col("BSE_YW").as("baseYearWeek"), col("OFER_CHNG_SEQ").as("offerChangeSeq"), col("OFER_PRCE").as("dealPrice"), col("OFER_QTY").as("offerQty"), col("OFER_REMN_QTY").as("leftQty"), col("DEAL_NR").as("referenceEventNumber"), col("DEAL_CHNG_SEQ").as("referenceEventChangeSeq")).withColumn("dealQty", col("offerQty") - col("leftQty")).withColumn("dealAmt", col("dealQty") * col("dealPrice")).withColumn("leftPrice", col("dealPrice")).withColumn("leftAmt", col("leftPrice") * col("leftQty")).withColumn("offerChangeSeq", col("offerChangeSeq") + 1)

    lazy val idxList = srcDealLineItem.groupBy("offerNumber", "baseYearWeek").agg(max("offerChangeSeq").as("offerChangeSeq"))
    lazy val agged1 = idxList.join(srcDealLineItem, Seq("offerNumber", "offerChangeSeq", "baseYearWeek"), "left").drop("referenceEventNumber", "referenceEventChangeSeq")
      .withColumn("tradeClosing", lit("20220101010101010000"))
      .withColumn("aggDealQty", col("dealQty")).drop("dealQty")
      .withColumn("aggDealPrice", col("dealPrice")).drop("dealPrice")
      .withColumn("aggDealAmt", col("dealAmt")).drop("dealAmt")
      .withColumn("aggLeftQty", col("leftQty")).drop("leftQty")
      .withColumn("aggLeftPrice", col("leftPrice")).drop("leftPrice")
      .withColumn("aggLeftAmt", col("leftAmt")).drop("leftAmt")
    lazy val agged2 = srcDealLineItem.join(srcDeal, Seq("referenceEventNumber", "referenceEventChangeSeq"), "left").filter(col("dealQty") > 0).groupBy("offerNumber", "baseYearWeek", "offerChangeSeq").agg(collect_list(struct("eventTimestamp", "referenceEventNumber", "referenceEventChangeSeq", "dealQty", "dealPrice", "dealAmt", "offerQty", "leftQty", "leftPrice", "leftAmt")).as("dealLog"))

    val F9S_DSBD_WKDETAIL = agged1.join(agged2, Seq("offerNumber", "baseYearWeek", "offerChangeSeq"), "left")

//    F9S_DSBD_WKDETAIL.repartition(1).write.mode("append").json(pathJsonSave + "/F9S_DSBD_WKDETAIL")

//    F9S_DSBD_WKDETAIL.write.mode("append").parquet(pathParquetSave + "/F9S_DSBD_WKDETAIL")
    MongoSpark.save(F9S_DSBD_WKDETAIL.write
      .option("uri", "mongodb://data.freight9.com/f9s")
      .option("collection", "F9S_DSBD_WKDETAIL").mode("overwrite"))
  }
}
