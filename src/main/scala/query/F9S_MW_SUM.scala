package query

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

case class F9S_MW_SUM(var spark: SparkSession, var pathSourceFrom: String,
                      var pathParquetSave: String, var pathJsonSave: String, var currentWk: String) {
  def mw_sum(): Unit = {
    println("////////////////////////////////MW SUM: JOB STARTED////////////////////////////////////////")
    lazy val F9S_STATS_RAW = spark.read.parquet(pathParquetSave + "/F9S_STATS_RAW")
    lazy val FTR_DEAL_CRYR = spark.read.parquet(pathSourceFrom + "/FTR_DEAL_CRYR")

    lazy val companyCodes = FTR_DEAL_CRYR.groupBy("OFER_NR", "OFER_CHNG_SEQ").agg(collect_list("OFER_CRYR_CD").as("companyCodes"))
    lazy val polData = F9S_STATS_RAW.join(companyCodes, Seq("OFER_NR", "OFER_CHNG_SEQ"), "left")
      .filter(col("TRDE_LOC_TP_CD") === "02")
      .withColumn("polCode", col("TRDE_LOC_CD"))
      .drop("DEAL_NR", "TRDE_LOC_TP_CD", "TRDE_LOC_CD", "DEAL_CHNG_SEQ", "DEAL_SKIP_YN", "DEAL_SUCC_YN", "REG_SEQ", "a1", "a2", "companyCodes", "OFFER_REG_SEQ")
    lazy val podData = F9S_STATS_RAW.join(companyCodes, Seq("OFER_NR", "OFER_CHNG_SEQ"), "left")
      .filter(col("TRDE_LOC_TP_CD") === "03")
      .withColumn("podCode", col("TRDE_LOC_CD"))
      .drop("DEAL_NR", "TRDE_LOC_TP_CD", "TRDE_LOC_CD", "DEAL_CHNG_SEQ", "DEAL_SKIP_YN", "DEAL_SUCC_YN", "REG_SEQ", "DEAL_DATE", "DEAL_YEAR", "DEAL_MONTH", "DEAL_DAY", "DEAL_HOUR", "DEAL_MIN", "DEAL_SEC", "DEAL_AMT", "DEAL_PRCE", "DEAL_QTY")
    val aggData = polData.join(podData, Seq("OFER_NR", "OFER_CHNG_SEQ", "DEAL_YW"), "left")
      .withColumn("baseYearWeek", col("DEAL_YW")).drop("DEAL_YW")
      .withColumn("topicName", lit("test"))
      .withColumn("timestamp", lit("test"))
      .withColumn("marketTypeCode", lit("01"))
      .withColumn("rdtermCode", lit("01"))
      .withColumn("containerTypeCode", lit("01"))
      .withColumn("paymentTermCode", lit("01"))
      .withColumn("qtyUnit", lit("T"))
      .withColumn("latestEventTimestamp", col("DEAL_DATE")).drop("DEAL_DATE")

    lazy val openPrce = aggData.withColumn("numerator", row_number.over(Window.partitionBy(col("marketTypeCode"), col("rdTermCode"), col("containerTypeCode"), col("paymentTermCode"), col("polCode"), col("podCode"), col("qtyUnit"), col("baseYearWeek")).orderBy(col("latestEventTimestamp").asc)))
      .filter(col("numerator") === 1)
      .withColumn("open", col("DEAL_PRCE"))
      .select("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit", "baseYearWeek", "open")

    lazy val closePrce = aggData.withColumn("numerator", row_number.over(Window.partitionBy(col("marketTypeCode"), col("rdTermCode"), col("containerTypeCode"), col("paymentTermCode"), col("polCode"), col("podCode"), col("qtyUnit"), col("baseYearWeek")).orderBy(col("latestEventTimestamp").desc)))
      .filter(col("numerator") === 1)
      .withColumn("close", col("DEAL_PRCE"))
      .select("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit", "baseYearWeek", "close")

    lazy val tradeStatus = openPrce.join(closePrce, Seq("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit", "baseYearWeek"), "left")
      .withColumn("status", when(col("baseYearWeek") > currentWk, lit("On Market")).otherwise("Trading Closed"))
    lazy val changeValue = tradeStatus.withColumn("tmp", lag(col("close"), 1, 0).over(Window.partitionBy(col("polCode"), col("podCode")).orderBy(col("polCode").asc, col("podCode").asc, col("baseYearWeek").asc)))
      .withColumn("changeValue", col("close") - col("tmp")).drop("tmp")
    lazy val changeRate = changeValue.withColumn("changeRate", col("changeValue") / col("close"))

    val F9S_MW_SUM = aggData.groupBy("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit", "baseYearWeek")
      .agg(max("DEAL_PRCE").as("high"), min("DEAL_PRCE").as("low"), max("latestEventTimeStamp").as("latestEventTimeStamp"), sum("DEAL_QTY").as("volume"))
      .join(changeRate, Seq("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit", "baseYearWeek"), "left")
      .groupBy("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit")
      .agg(collect_list(struct("baseYearWeek", "open", "high", "low", "close", "status", "changeValue", "changeRate", "latestEventTimeStamp", "volume")).as("Cell"))
      .drop("baseYearWeek", "open", "high", "low", "close", "status", "changeValue", "changeRate", "latestEventTimeStamp", "volume")

    F9S_MW_SUM.repartition(1).write.mode("append").json(pathJsonSave + "/F9S_MW_SUM")

//    F9S_MW_SUM.write.mode("append").parquet(pathParquetSave + "/F9S_MW_SUM")
    F9S_MW_SUM.printSchema
    println("/////////////////////////////JOB FINISHED//////////////////////////////")
  }
}
