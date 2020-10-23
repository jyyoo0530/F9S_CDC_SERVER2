package f9s.core.query

import com.mongodb.spark.MongoSpark
import f9s.{appConf, hadoopConf, mongoConf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._


case class F9S_MW_HST(var spark: SparkSession) {

  def mw_hst(srcData: DataFrame): DataFrame = {
    println("////////////////////////////////MW HST: JOB STARTED////////////////////////////////////////")
    //////DATA LOAD////
    lazy val F9S_STATS_RAW = srcData

    ///////SQL//////////
    lazy val F9S_MW_HST =
      F9S_STATS_RAW
        .drop("carrierCode")
        .withColumn("referenceEventNumber", col("DEAL_NR")).drop("DEAL_NR")
        .withColumn("referenceEventChangeNumber", col("DEAL_CHNG_SEQ")).drop("DEAL_CHNG_SEQ")
        .withColumn("timestamp", col("DEAL_DT")).drop("DEAL_DT")
        .withColumn("containerTypeCode", lit("01"))
        .withColumn("qtyUnit", lit("T"))
        .filter(col("dealQty") =!= 0 and col("offerTypeCode") === "S").distinct
        .withColumn("laggedPrice", lag(col("dealPrice"), 1, 0).over(Window.partitionBy("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit", "baseYearWeek").orderBy("timestamp")))
        .withColumn("priceChange", col("dealPrice").minus(col("laggedPrice"))).drop("laggedPrice")
        .withColumn("priceRate", col("priceChange").divide(col("dealPrice")))
        .withColumn("idx", row_number.over(Window.partitionBy("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit", "baseYearWeek").orderBy(col("timestamp").asc)))
        .groupBy("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit", "baseYearWeek")
        .agg(collect_set(struct(col("idx"),
          col("timestamp"),
          col("referenceEventNumber"),
          col("referenceEventChangeNumber"),
          col("dealQty"),
          col("dealPrice"),
          col("dealAmt"),
          col("priceChange"),
          col("priceRate"))).as("Cell"))
        .distinct

    ////////// DataLake -> 2nd Tier ////////
    F9S_MW_HST.printSchema

    println("/////////////////////////////JOB FINISHED//////////////////////////////")

    F9S_MW_HST
  }

  def append_mw_hst(srcData: DataFrame, topicList: DataFrame): DataFrame = {
    println("////////////////////////////////MW HST: JOB STARTED////////////////////////////////////////")
    //////DATA LOAD////
    lazy val F9S_STATS_RAW = topicList.join(srcData, Seq("polCode", "podCode", "baseYearWeek"), "left")

    ///////SQL//////////
    lazy val F9S_MW_HST =
      F9S_STATS_RAW
        .drop("carrierCode")
        .withColumn("referenceEventNumber", col("DEAL_NR")).drop("DEAL_NR")
        .withColumn("referenceEventChangeNumber", col("DEAL_CHNG_SEQ")).drop("DEAL_CHNG_SEQ")
        .withColumn("timestamp", col("DEAL_DT")).drop("DEAL_DT")
        .withColumn("containerTypeCode", lit("01"))
        .withColumn("qtyUnit", lit("T"))
        .filter(col("dealQty") =!= 0 and col("offerTypeCode") === "S").distinct
        .withColumn("laggedPrice", lag(col("dealPrice"), 1, 0).over(Window.partitionBy("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit", "baseYearWeek").orderBy("timestamp")))
        .withColumn("priceChange", col("dealPrice").minus(col("laggedPrice"))).drop("laggedPrice")
        .withColumn("priceRate", col("priceChange").divide(col("dealPrice")))
        .withColumn("idx", row_number.over(Window.partitionBy("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit", "baseYearWeek").orderBy(col("timestamp").asc)))
        .groupBy("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit", "baseYearWeek")
        .agg(collect_set(struct(col("idx"),
          col("timestamp"),
          col("referenceEventNumber"),
          col("referenceEventChangeNumber"),
          col("dealQty"),
          col("dealPrice"),
          col("dealAmt"),
          col("priceChange"),
          col("priceRate"))).as("Cell"))
        .distinct

    ////////// DataLake -> 2nd Tier ////////

    println("/////////////////////////////JOB FINISHED//////////////////////////////")

    F9S_MW_HST
  }
}
