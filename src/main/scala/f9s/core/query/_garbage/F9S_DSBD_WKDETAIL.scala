package f9s.core.query._garbage

import com.mongodb.spark.MongoSpark
import f9s.{appConf, hadoopConf, mongoConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

case class F9S_DSBD_WKDETAIL(var spark: SparkSession) {

  val filePath = appConf().dataLake match {
    case "file" => appConf().folderOrigin
    case "hadoop" => hadoopConf.hadoopPath
  }

  def dsbd_wkdetail(): Unit = {
    println("////////////////////////////////DSBD WKDETAIL: JOB STARTED////////////////////////////////////////")
    lazy val weektable = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(filePath + "/weektable.csv")
      .select(col("BSE_YW").cast("String"), col("yyyymmdd").cast("String")).withColumn("timestamp", concat(col("yyyymmdd"), lit("010000000000"))).drop("yyyymmdd")
    lazy val FTR_DEAL = spark.read.parquet(filePath + "/FTR_DEAL")
      .select("DEAL_DT", "DEAL_NR", "DEAL_CHNG_SEQ", "OFER_NR", "OFER_CHNG_SEQ")
      .withColumn("offerChangeSeq_forjoin", col("OFER_CHNG_SEQ") + 1).drop("OFER_CHNG_SEQ")
    lazy val FTR_OFER_LINE_ITEM = spark.read.parquet(filePath + "/FTR_OFER_LINE_ITEM")
      .withColumn("offerChangeSeq_forjoin", col("OFER_CHNG_SEQ"))
      .join(FTR_DEAL, Seq("OFER_NR", "offerChangeSeq_forjoin"), "left").drop("offerChangeSeq_forjoin")
      .join(weektable, Seq("BSE_YW"), "left")
      .withColumn("laggedLeftQty", lag(col("OFER_REMN_QTY"), 1, 0).over(Window.partitionBy("OFER_NR", "BSE_YW").orderBy("OFER_CHNG_SEQ")))

    lazy val src = FTR_OFER_LINE_ITEM
      .select(col("OFER_NR").as("offerNumber"),
        col("BSE_YW").as("baseYearWeek"),
        col("OFER_CHNG_SEQ").as("offerChangeSeq"),
        col("timestamp").as("tradeClosing"),
        col("DEAL_DT").as("eventTimestamp"),
        col("DEAL_NR").as("referenceEventNumber"),
        col("DEAL_CHNG_SEQ").as("referenceEventChangeSeq"),
        col("OFER_QTY").as("offerQty"),
        col("OFER_REMN_QTY").as("leftQty"),
        col("OFER_PRCE").as("leftPrice"),
        col("laggedLeftQty"),
        col("DEAL_PRCE")
      )
      .withColumn("leftAmt", when(col("leftQty") =!= 0, col("leftQty").multiply(col("leftPrice"))).otherwise(0))
      .withColumn("dealQty", when(col("offerChangeSeq") === 0, lit(0)).otherwise(col("laggedLeftQty").minus(col("leftQty")))).drop("laggedLeftQty")
      .withColumn("dealPrice", col("DEAL_PRCE")).drop("DEAL_PRCE")
      .withColumn("dealAmt", col("dealQty").multiply(col("dealPrice")))
      .withColumn("aggDealQty", lit(sum("dealQty").over(Window.partitionBy("offerNumber", "baseYearWeek"))))
      .withColumn("aggDealAmt", lit(sum("dealAmt").over(Window.partitionBy("offerNumber", "baseYearWeek"))))
      .withColumn("aggDealPrice", col("aggDealAmt").divide(col("aggDealQty")))
      .withColumn("aggOfferQty", lit(sum("offerQty").over(Window.partitionBy("offerNumber", "baseYearWeek"))))
      .withColumn("aggLeftQty", lit(min("leftQty").over(Window.partitionBy("offerNumber", "baseYearWeek"))))
      .withColumn("aggLeftAmt", lit(min("leftAmt").over(Window.partitionBy("offerNumber", "baseYearWeek"))))
      .withColumn("aggLeftPrice", col("leftPrice"))
      .withColumn("maxOfferChangeSeq", lit(max("offerChangeSeq").over(Window.partitionBy("offerNumber", "baseYearWeek"))))
      .filter(col("maxOfferChangeSeq") === col("offerChangeSeq")).drop("maxOfferChangeSeq")
      .filter(col("offerChangeSeq") === 0 or (col("offerChangeSeq") =!= 0 and col("dealQty") > 0))

    val F9S_DSBD_WKDETAIL = src.groupBy("offerNumber",
      "baseYearWeek",
      "offerChangeSeq",
      "aggDealQty",
      "aggDealPrice",
      "aggDealAmt",
      "aggOfferQty",
      "aggLeftQty",
      "aggLeftPrice",
      "aggLeftAmt",
      "tradeClosing")
      .agg(
        collect_set(struct("eventTimestamp",
          "referenceEventNumber",
          "referenceEventChangeSeq",
          "dealQty", "dealPrice",
          "dealAmt",
          "offerQty",
          "leftQty",
          "leftPrice",
          "leftAmt")).as("dealLog"))

    F9S_DSBD_WKDETAIL.printSchema

    //    F9S_DSBD_WKDETAIL.repartition(5).write.mode("append").json(pathJsonSave + "/F9S_DSBD_WKDETAIL")
    //    F9S_DSBD_WKDETAIL.write.mode("append").parquet(pathParquetSave + "/F9S_DSBD_WKDETAIL")

    MongoSpark.save(F9S_DSBD_WKDETAIL.write
      .option("uri", mongoConf.sparkMongoUri)
      .option("database", "f9s")
      .option("collection", "F9S_DSBD_WKDETAIL").mode("overwrite"))
    println("/////////////////////////////JOB FINISHED//////////////////////////////")
  }

  def append_dsbd_wkdetail(offerNumbers: Seq[String]): Unit = {
    println("////////////////////////////////DSBD WKDETAIL: JOB STARTED////////////////////////////////////////")
    lazy val weektable = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(filePath + "/weektable.csv")
      .select(col("BSE_YW").cast("String"), col("yyyymmdd").cast("String"))
      .withColumn("timestamp", concat(col("yyyymmdd"), lit("010000000000")))
      .drop("yyyymmdd")
    lazy val FTR_DEAL = spark.read.parquet(filePath + "/FTR_DEAL")
      .filter(col("OFER_NR") isin (offerNumbers: _*))
      .select("DEAL_DT", "DEAL_NR", "DEAL_CHNG_SEQ", "OFER_NR", "OFER_CHNG_SEQ")
      .withColumn("offerChangeSeq_forjoin", col("OFER_CHNG_SEQ") + 1).drop("OFER_CHNG_SEQ")
    lazy val FTR_OFER_LINE_ITEM = spark.read.parquet(filePath + "/FTR_OFER_LINE_ITEM")
      .filter(col("OFER_NR") isin (offerNumbers: _*))
      .withColumn("offerChangeSeq_forjoin", col("OFER_CHNG_SEQ"))
      .join(FTR_DEAL, Seq("OFER_NR", "offerChangeSeq_forjoin"), "left").drop("offerChangeSeq_forjoin")
      .join(weektable, Seq("BSE_YW"), "left")
      .withColumn("laggedLeftQty", lag(col("OFER_REMN_QTY"), 1, 0).over(Window.partitionBy("OFER_NR", "BSE_YW").orderBy("OFER_CHNG_SEQ")))

    lazy val src = FTR_OFER_LINE_ITEM
      .select(col("OFER_NR").as("offerNumber"),
        col("BSE_YW").as("baseYearWeek"),
        col("OFER_CHNG_SEQ").as("offerChangeSeq"),
        col("timestamp").as("tradeClosing"),
        col("DEAL_DT").as("eventTimestamp"),
        col("DEAL_NR").as("referenceEventNumber"),
        col("DEAL_CHNG_SEQ").as("referenceEventChangeSeq"),
        col("OFER_QTY").as("offerQty"),
        col("OFER_REMN_QTY").as("leftQty"),
        col("OFER_PRCE").as("leftPrice"),
        col("laggedLeftQty"),
        col("DEAL_PRCE")
      )
      .withColumn("leftAmt", when(col("leftQty") =!= 0, col("leftQty").multiply(col("leftPrice"))).otherwise(0))
      .withColumn("dealQty", when(col("offerChangeSeq") === 0, lit(0)).otherwise(col("laggedLeftQty").minus(col("leftQty")))).drop("laggedLeftQty")
      .withColumn("dealPrice", col("DEAL_PRCE")).drop("DEAL_PRCE")
      .withColumn("dealAmt", col("dealQty").multiply(col("dealPrice")))
      .withColumn("aggDealQty", lit(sum("dealQty").over(Window.partitionBy("offerNumber", "baseYearWeek"))))
      .withColumn("aggDealAmt", lit(sum("dealAmt").over(Window.partitionBy("offerNumber", "baseYearWeek"))))
      .withColumn("aggDealPrice", col("aggDealAmt").divide(col("aggDealQty")))
      .withColumn("aggOfferQty", lit(sum("offerQty").over(Window.partitionBy("offerNumber", "baseYearWeek"))))
      .withColumn("aggLeftQty", lit(min("leftQty").over(Window.partitionBy("offerNumber", "baseYearWeek"))))
      .withColumn("aggLeftAmt", lit(min("leftAmt").over(Window.partitionBy("offerNumber", "baseYearWeek"))))
      .withColumn("aggLeftPrice", col("leftPrice"))
      .withColumn("maxOfferChangeSeq", lit(max("offerChangeSeq").over(Window.partitionBy("offerNumber", "baseYearWeek"))))
      .filter(col("maxOfferChangeSeq") === col("offerChangeSeq")).drop("maxOfferChangeSeq")
      .filter(col("offerChangeSeq") === 0 or (col("offerChangeSeq") =!= 0 and col("dealQty") > 0))

    val F9S_DSBD_WKDETAIL = src.groupBy("offerNumber",
      "baseYearWeek",
      "offerChangeSeq",
      "aggDealQty",
      "aggDealPrice",
      "aggDealAmt",
      "aggOfferQty",
      "aggLeftQty",
      "aggLeftPrice",
      "aggLeftAmt",
      "tradeClosing")
      .agg(
        collect_set(struct("eventTimestamp",
          "referenceEventNumber",
          "referenceEventChangeSeq",
          "dealQty", "dealPrice",
          "dealAmt",
          "offerQty",
          "leftQty",
          "leftPrice",
          "leftAmt")).as("dealLog"))

    F9S_DSBD_WKDETAIL.printSchema

    //    F9S_DSBD_WKDETAIL.repartition(5).write.mode("append").json(pathJsonSave + "/F9S_DSBD_WKDETAIL")
    //    F9S_DSBD_WKDETAIL.write.mode("append").parquet(pathParquetSave + "/F9S_DSBD_WKDETAIL")

    MongoSpark.save(F9S_DSBD_WKDETAIL.write
      .option("uri", mongoConf.sparkMongoUri)
      .option("database", "f9s")
      .option("collection", "F9S_DSBD_WKDETAIL").mode("append"))
    println("/////////////////////////////JOB FINISHED//////////////////////////////")
  }
}
