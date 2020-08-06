package f9s.core.query

import com.mongodb.spark.MongoSpark
import f9s.{hadoopConf, mongoConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

case class F9S_MW_SUM(var spark: SparkSession, var pathSourceFrom: String,
                      var pathParquetSave: String, var pathJsonSave: String, var currentWk: String) {
  def mw_sum(): Unit = {
    println("////////////////////////////////MW SUM: JOB STARTED////////////////////////////////////////")
    lazy val FTR_DEAL = spark.read.parquet(hadoopConf.hadoopPath + "/FTR_DEAL")
    lazy val F9S_STATS_RAW = spark.read.parquet(hadoopConf.hadoopPath + "/F9S_STATS_RAW")

    lazy val srcAgged = FTR_DEAL.filter(col("OFER_TP_CD") === "S").select(
      col("TRDE_MKT_TP_CD").as("marketTypeCode"),
      col("OFER_RD_TRM_CD").as("rdTermCode"),
      col("OFER_PYMT_TRM_CD").as("paymentTermCode"),
      col("DEAL_NR"),
      col("DEAL_CHNG_SEQ"))
      .withColumn("containerTypeCode", lit("01"))
      .withColumn("qtyUnit", lit("T"))
      .join(F9S_STATS_RAW
        .withColumn("status", when(col("BSE_YW") >= currentWk, lit("on Market")).otherwise(lit("Trade Closed")))
        .filter(col("DEAL_QTY") =!= 0),
        Seq("DEAL_NR", "DEAL_CHNG_SEQ"),
        "left").drop("DEAL_NR", "DEAL_CHNG_SEQ")
      .withColumn("baseYearWeek", col("BSE_YW")).drop("BSE_YW")
      .withColumn("openChk", row_number().over(Window.partitionBy("marketTypeCode",
        "rdTermCode",
        "containerTypeCode",
        "paymentTermCode",
        "polCode",
        "podCode",
        "qtyUnit",
        "baseYearWeek",
        "status").orderBy(col("DEAL_DT").asc)))
      .withColumn("closeChk", row_number().over(Window.partitionBy("marketTypeCode",
        "rdTermCode",
        "containerTypeCode",
        "paymentTermCode",
        "polCode",
        "podCode",
        "qtyUnit",
        "baseYearWeek",
        "status").orderBy(col("DEAL_DT").desc)))
      .withColumn("open", when(col("openChk") === 1, col("DEAL_PRCE")
      ).otherwise(lit(0)))
      .withColumn("close", when(col("closeChk") === 1, col("DEAL_PRCE")
      ).otherwise(lit(0))).drop("openChk", "closeChk")
      .groupBy("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit", "baseYearWeek", "status")
      .agg(max("DEAL_PRCE").as("high"),
        min("DEAL_PRCE").as("low"),
        sum("DEAL_QTY").as("volume"),
        max("DEAL_DT").as("latestEventDate"),
        sum("open").as("open"),
        sum("close").as("close"))
      .withColumn("laggedPrice", lag(col("close"), 1, 0).over(Window.partitionBy("marketTypeCode",
        "rdTermCode",
        "containerTypeCode",
        "paymentTermCode",
        "polCode",
        "podCode",
        "qtyUnit",
        "baseYearWeek",
        "status").orderBy("baseYearWeek")))
      .withColumn("changeValue", col("close").minus(col("laggedPrice")))
      .withColumn("changeRate", col("changeValue").divide(col("close")))


    val F9S_MW_SUM = srcAgged.groupBy("marketTypeCode",
      "rdTermCode",
      "containerTypeCode",
      "paymentTermCode",
      "polCode",
      "podCode",
      "qtyUnit")
      .agg(collect_set(struct("baseYearWeek",
        "status",
        "open",
        "low",
        "high",
        "close",
        "volume",
        "changeValue",
        "changeRate",
        "latestEventDate")).as("Cell"))

    //        F9S_MW_SUM.repartition(5).write.mode("append").json(pathJsonSave + "/F9S_MW_SUM")
    //    F9S_MW_SUM.write.mode("append").parquet(pathParquetSave + "/F9S_MW_SUM")
    MongoSpark.save(F9S_MW_SUM.write
      .option("uri", mongoConf.sparkMongoUri)
      .option("database", "f9s")
      .option("collection", "F9S_MW_SUM").mode("overwrite"))
    F9S_MW_SUM.printSchema
    println("/////////////////////////////JOB FINISHED//////////////////////////////")
  }

  def append_mw_sum(): Unit = {
    println("////////////////////////////////MW SUM: JOB STARTED////////////////////////////////////////")
    lazy val FTR_DEAL = spark.read.parquet(hadoopConf.hadoopPath + "/FTR_DEAL")
    lazy val F9S_STATS_RAW = spark.read.parquet(hadoopConf.hadoopPath + "/F9S_STATS_RAW")

    lazy val srcAgged = FTR_DEAL.filter(col("OFER_TP_CD") === "S").select(
      col("TRDE_MKT_TP_CD").as("marketTypeCode"),
      col("OFER_RD_TRM_CD").as("rdTermCode"),
      col("OFER_PYMT_TRM_CD").as("paymentTermCode"),
      col("DEAL_NR"),
      col("DEAL_CHNG_SEQ"))
      .withColumn("containerTypeCode", lit("01"))
      .withColumn("qtyUnit", lit("T"))
      .join(F9S_STATS_RAW
        .withColumn("status", when(col("BSE_YW") >= currentWk, lit("on Market")).otherwise(lit("Trade Closed")))
        .filter(col("DEAL_QTY") =!= 0),
        Seq("DEAL_NR", "DEAL_CHNG_SEQ"),
        "left").drop("DEAL_NR", "DEAL_CHNG_SEQ")
      .withColumn("baseYearWeek", col("BSE_YW")).drop("BSE_YW")
      .withColumn("openChk", row_number().over(Window.partitionBy("marketTypeCode",
        "rdTermCode",
        "containerTypeCode",
        "paymentTermCode",
        "polCode",
        "podCode",
        "qtyUnit",
        "baseYearWeek",
        "status").orderBy(col("DEAL_DT").asc)))
      .withColumn("closeChk", row_number().over(Window.partitionBy("marketTypeCode",
        "rdTermCode",
        "containerTypeCode",
        "paymentTermCode",
        "polCode",
        "podCode",
        "qtyUnit",
        "baseYearWeek",
        "status").orderBy(col("DEAL_DT").desc)))
      .withColumn("open", when(col("openChk") === 1, col("DEAL_PRCE")
      ).otherwise(lit(0)))
      .withColumn("close", when(col("closeChk") === 1, col("DEAL_PRCE")
      ).otherwise(lit(0))).drop("openChk", "closeChk")
      .groupBy("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit", "baseYearWeek", "status")
      .agg(max("DEAL_PRCE").as("high"),
        min("DEAL_PRCE").as("low"),
        sum("DEAL_QTY").as("volume"),
        max("DEAL_DT").as("latestEventDate"),
        sum("open").as("open"),
        sum("close").as("close"))
      .withColumn("laggedPrice", lag(col("close"), 1, 0).over(Window.partitionBy("marketTypeCode",
        "rdTermCode",
        "containerTypeCode",
        "paymentTermCode",
        "polCode",
        "podCode",
        "qtyUnit",
        "baseYearWeek",
        "status").orderBy("baseYearWeek")))
      .withColumn("changeValue", col("close").minus(col("laggedPrice")))
      .withColumn("changeRate", col("changeValue").divide(col("close")))


    val F9S_MW_SUM = srcAgged.groupBy("marketTypeCode",
      "rdTermCode",
      "containerTypeCode",
      "paymentTermCode",
      "polCode",
      "podCode",
      "qtyUnit")
      .agg(collect_set(struct("baseYearWeek",
        "status",
        "open",
        "low",
        "high",
        "close",
        "volume",
        "changeValue",
        "changeRate",
        "latestEventDate")).as("Cell"))

    //        F9S_MW_SUM.repartition(5).write.mode("append").json(pathJsonSave + "/F9S_MW_SUM")
    //    F9S_MW_SUM.write.mode("append").parquet(pathParquetSave + "/F9S_MW_SUM")
    MongoSpark.save(F9S_MW_SUM.write
      .option("uri", mongoConf.sparkMongoUri)
      .option("database", "f9s")
      .option("collection", "F9S_MW_SUM").mode("append"))
    F9S_MW_SUM.printSchema
    println("/////////////////////////////JOB FINISHED//////////////////////////////")
  }
}
