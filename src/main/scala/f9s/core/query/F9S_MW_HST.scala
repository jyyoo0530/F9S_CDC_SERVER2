package f9s.core.query

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._


case class F9S_MW_HST(var spark: SparkSession, var pathSourceFrom: String,
                      var pathParquetSave: String, var pathJsonSave: String) {
  def mw_hst(): Unit = {
    println("////////////////////////////////MW HST: JOB STARTED////////////////////////////////////////")
    lazy val FTR_DEAL = spark.read.parquet(pathSourceFrom + "/FTR_DEAL")
      .select(col("DEAL_NR").as("referenceEventNumber"),
        col("DEAL_CHNG_SEQ").as("referenceEventChangeNumber"),
        col("TRDE_MKT_TP_CD").as("marketTypeCode"),
        col("OFER_PYMT_TRM_CD").as("paymentTermCode"),
        col("OFER_RD_TRM_CD").as("rdTermCode"),
        col("DEAL_DT").as("timestamp")
      ).distinct
    lazy val F9S_STATS_RAW = spark.read.parquet(pathParquetSave + "/F9S_STATS_RAW")
      .select(
        col("DEAL_NR").as("referenceEventNumber"),
        col("DEAL_CHNG_SEQ").as("referenceEventChangeNumber"),
        col("BSE_YW").as("baseYearWeek"),
        col("DEAL_QTY").as("dealQty"),
        col("DEAL_PRCE").as("dealPrice"),
        col("DEAL_QTY").multiply(col("DEAL_PRCE")).as("dealAmt"),
        col("OFER_TP_CD").as("offerTypeCode"),
        col("polCode"),
        col("podCode"))
      .withColumn("containerTypeCode", lit("01"))
      .withColumn("qtyUnit", lit("T"))
      .filter(col("dealQty") =!= 0 and col("offerTypeCode") === "S").distinct

    lazy val F9S_MW_HST = F9S_STATS_RAW.join(FTR_DEAL, Seq("referenceEventNumber", "referenceEventChangeNumber"), "left").distinct
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


    //    F9S_MW_HST.repartition(50).write.mode("append").json(pathJsonSave + "/F9S_MW_HST")

    //    F9S_MW_HST.write.mode("append").parquet(pathParquetSave+"/F9S_MW_HST")
    MongoSpark.save(F9S_MW_HST.write
      .option("uri", "mongodb://ec2-13-209-15-68.ap-northeast-2.compute.amazonaws.com:27017/f9s")
      .option("collection", "F9S_MW_HST").mode("overwrite"))
    F9S_MW_HST.printSchema

    println("/////////////////////////////JOB FINISHED//////////////////////////////")
  }

  def append_mw_hst(): Unit = {
    println("////////////////////////////////MW HST: JOB STARTED////////////////////////////////////////")
    lazy val FTR_DEAL = spark.read.parquet(pathSourceFrom + "/FTR_DEAL")
      .select(col("DEAL_NR").as("referenceEventNumber"),
        col("DEAL_CHNG_SEQ").as("referenceEventChangeNumber"),
        col("TRDE_MKT_TP_CD").as("marketTypeCode"),
        col("OFER_PYMT_TRM_CD").as("paymentTermCode"),
        col("OFER_RD_TRM_CD").as("rdTermCode"),
        col("DEAL_DT").as("timestamp")
      ).distinct
    lazy val F9S_STATS_RAW = spark.read.parquet(pathParquetSave + "/F9S_STATS_RAW")
      .select(
        col("DEAL_NR").as("referenceEventNumber"),
        col("DEAL_CHNG_SEQ").as("referenceEventChangeNumber"),
        col("BSE_YW").as("baseYearWeek"),
        col("DEAL_QTY").as("dealQty"),
        col("DEAL_PRCE").as("dealPrice"),
        col("DEAL_QTY").multiply(col("DEAL_PRCE")).as("dealAmt"),
        col("OFER_TP_CD").as("offerTypeCode"),
        col("polCode"),
        col("podCode"))
      .withColumn("containerTypeCode", lit("01"))
      .withColumn("qtyUnit", lit("T"))
      .filter(col("dealQty") =!= 0 and col("offerTypeCode") === "S").distinct

    lazy val F9S_MW_HST = F9S_STATS_RAW.join(FTR_DEAL, Seq("referenceEventNumber", "referenceEventChangeNumber"), "left").distinct
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


    //    F9S_MW_HST.repartition(50).write.mode("append").json(pathJsonSave + "/F9S_MW_HST")

    //    F9S_MW_HST.write.mode("append").parquet(pathParquetSave+"/F9S_MW_HST")
    MongoSpark.save(F9S_MW_HST.write
      .option("uri", "mongodb://ec2-13-209-15-68.ap-northeast-2.compute.amazonaws.com:27017/f9s")
      .option("collection", "F9S_MW_HST").mode("overwrite"))
    F9S_MW_HST.printSchema

    println("/////////////////////////////JOB FINISHED//////////////////////////////")
  }
}
