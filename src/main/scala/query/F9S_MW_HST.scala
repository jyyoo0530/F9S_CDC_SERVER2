package query

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._


case class F9S_MW_HST(var spark: SparkSession, var pathSourceFrom: String,
                      var pathParquetSave: String, var pathJsonSave: String) {
  def mw_hst(): Unit = {
    println("////////////////////////////////MW HST: JOB STARTED////////////////////////////////////////")
    lazy val F9S_STATS_RAW = spark.read.parquet(pathParquetSave + "/F9S_STATS_RAW")
    lazy val FTR_DEAL_CRYR = spark.read.parquet(pathSourceFrom + "/FTR_DEAL_CRYR")

    lazy val companyCodes = FTR_DEAL_CRYR.groupBy("OFER_NR", "OFER_CHNG_SEQ").agg(collect_list("OFER_CRYR_CD").as("companyCodes"))
    lazy val polData = F9S_STATS_RAW.filter(col("TRDE_LOC_TP_CD") === "02")
      .withColumn("polCode", col("TRDE_LOC_CD"))
      .drop("DEAL_NR", "TRDE_LOC_TP_CD", "TRDE_LOC_CD", "DEAL_CHNG_SEQ", "DEAL_SKIP_YN", "DEAL_SUCC_YN", "REG_SEQ", "a1", "a2", "OFFER_REG_SEQ")
    lazy val podData = F9S_STATS_RAW.filter(col("TRDE_LOC_TP_CD") === "03")
      .withColumn("podCode", col("TRDE_LOC_CD"))
      .drop("TRDE_LOC_TP_CD", "TRDE_LOC_CD", "DEAL_SKIP_YN", "DEAL_SUCC_YN", "REG_SEQ", "DEAL_DATE", "DEAL_YEAR", "DEAL_MONTH", "DEAL_DAY", "DEAL_HOUR", "DEAL_MIN", "DEAL_SEC", "DEAL_AMT", "DEAL_PRCE", "DEAL_QTY")
    lazy val rteData = polData.join(podData, Seq("OFER_NR", "OFER_CHNG_SEQ", "DEAL_YW"), "left")
      .drop("OFER_NR", "OFER_CHNG_SEQ", "OFFER_REG_SEQ", "a1", "a2")
      .withColumn("baseYearWeek", col("DEAL_YW")).drop("DEAL_YW")
      .withColumn("marketTypeCode", lit("01"))
      .withColumn("rdtermCode", lit("01"))
      .withColumn("containerTypeCode", lit("01"))
      .withColumn("paymentTermCode", lit("01"))
      .withColumn("qtyUnit", lit("T"))
      .withColumn("timestamp", col("DEAL_DATE")).drop("DEAL_DATE")
      .withColumn("referenceEventNumber", col("DEAL_NR")).drop("DEAL_NR")
      .withColumn("referenceEventChangeNumber", col("DEAL_CHNG_SEQ")).drop("DEAL_CHNG_SEQ")
      .withColumn("dealQty", col("DEAL_QTY")).drop("DEAL_QTY")
      .withColumn("dealPrice", col("DEAL_PRCE")).drop("DEAL_PRCE")
      .withColumn("dealAmt", col("DEAL_AMT")).drop("DEAL_AMT")
      .distinct
      .withColumn("idx", row_number.over(Window.partitionBy(col("marketTypeCode"), col("rdTermCode"), col("containerTypeCode"), col("paymentTermCode"), col("polCode"), col("podCode"), col("qtyUnit"), col("baseYearWeek")).orderBy(col("marketTypeCode").asc, col("rdTermCode").asc, col("containerTypeCode").asc, col("paymentTermCode").asc, col("polCode").asc, col("podCode").asc, col("qtyUnit").asc, col("baseYearWeek").asc, col("timestamp").asc)))

    lazy val priceChange = rteData.withColumn("tmp", lag(col("dealPrice"), 1, 0).over(Window.partitionBy(col("marketTypeCode"), col("rdTermCode"), col("containerTypeCode"), col("paymentTermCode"), col("polCode"), col("podCode"), col("qtyUnit"), col("baseYearWeek")).orderBy(col("marketTypeCode").asc, col("rdTermCode").asc, col("containerTypeCode").asc, col("paymentTermCode").asc, col("polCode").asc, col("podCode").asc, col("qtyUnit").asc, col("baseYearWeek").asc, col("timestamp").asc)))
      .withColumn("priceChange", col("dealPrice") - col("tmp")).drop("tmp")

    val aggData = priceChange.withColumn("priceRate", col("priceChange") / col("dealPrice")) // 중요

    lazy val listIdx = aggData.select("referenceEventNumber", "referenceEventChangeNumber", "marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit", "baseYearWeek").distinct

    val F9S_MW_HST = listIdx.join(aggData, Seq("referenceEventNumber", "referenceEventChangeNumber", "marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit", "baseYearWeek"), "left")
      .groupBy("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit", "baseYearWeek")
      .agg(collect_list(struct("idx", "timestamp", "referenceEventNumber", "referenceEventChangeNumber", "dealQty", "dealPrice", "dealAmt", "priceChange", "priceRate")).as("Cell"))
      .drop("timestamp", "referenceEventNumber", "referenceEventChangeNumber", "dealQty", "dealPrice", "dealAmt", "priceChange", "priceRate", "idx")

    lazy val mwidx = F9S_MW_HST.select("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "baseYearWeek").distinct.withColumn("writeIdx", concat(col("marketTypeCode"), col("rdTermCode"), col("containerTypeCode"), col("paymentTermCode"), col("polCode"), col("podCode"), col("baseYearWeek"))).withColumn("idx", row_number.over(Window.orderBy(col("writeIdx")))).withColumn("rte_idx", concat(col("polCode"), col("podCode")))
    lazy val idx = mwidx.collect()
    lazy val marketTypeCode = mwidx.select("marketTypeCode").collect().map(_ (0).toString)
    lazy val paymentTermCode = mwidx.select("paymentTermCode").collect().map(_ (0).toString)
    lazy val rdTermCode = mwidx.select("rdTermCode").collect().map(_ (0).toString)
    lazy val containerTypeCode = mwidx.select("containerTypeCode").collect().map(_ (0).toString)
    lazy val rte_idx = mwidx.select("rte_idx").collect().map(_ (0).toString)
    lazy val baseYearWeek = mwidx.select("baseYearWeek").collect().map(_ (0).toString)

    F9S_MW_HST.repartition(1).write.mode("append").json(pathJsonSave + "/F9S_MW_HST")

    //    F9S_MW_HST.write.mode("append").parquet(pathParquetSave+"/F9S_MW_HST")
    aggData.write.mode("append").parquet(pathParquetSave + "/aggData")
    MongoSpark.save(F9S_MW_HST.write
      .option("uri", "mongodb://data.freight9.com/f9s")
      .option("collection", "F9S_MW_HST").mode("overwrite"))
    F9S_MW_HST.printSchema
    aggData.printSchema
    println("/////////////////////////////JOB FINISHED//////////////////////////////")
  }
}
