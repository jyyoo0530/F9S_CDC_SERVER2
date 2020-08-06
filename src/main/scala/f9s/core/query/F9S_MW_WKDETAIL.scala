package f9s.core.query

import com.mongodb.spark.MongoSpark
import f9s.{hadoopConf, mongoConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

case class F9S_MW_WKDETAIL(var spark: SparkSession, var pathSourceFrom: String,
                           var pathParquetSave: String, var pathJsonSave: String) {
  def mw_wkdetail(): Unit = {
    println("////////////////////////////////MW WKDETAIL: JOB STARTED////////////////////////////////////////")

    lazy val FTR_DEAL = spark.read.parquet(hadoopConf.hadoopPath + "/FTR_DEAL")
      .select(col("DEAL_NR").as("dealNumber"),
        col("DEAL_CHNG_SEQ").as("dealChangeSeq"),
        col("TRDE_MKT_TP_CD").as("marketTypeCode"),
        col("OFER_PYMT_TRM_CD").as("paymentTermCode"),
        col("OFER_RD_TRM_CD").as("rdTermCode"),
        col("DEAL_DT").as("timestamp")
      )
    lazy val F9S_STATS_RAW = spark.read.parquet(hadoopConf.hadoopPath + "/F9S_STATS_RAW")
      .select(
        col("DEAL_NR").as("dealNumber"),
        col("DEAL_CHNG_SEQ").as("dealChangeSeq"),
        col("BSE_YW").as("baseYearWeek"),
        col("DEAL_QTY").as("volume"),
        col("DEAL_PRCE").as("dealPrice"),
        col("polCode"),
        col("podCode"))
      .withColumn("containerTypeCode", lit("01"))
      .withColumn("qtyUnit", lit("T"))
      .filter(col("dealPrice") =!= 0)

    lazy val src01 = F9S_STATS_RAW.join(FTR_DEAL, Seq("dealNumber", "dealChangeSeq"), "left")
      .withColumn("xAxis", col("timestamp").substr(lit(1), lit(8)))
      .withColumn("interval", lit("daily")) /// 루프포인트
      .withColumn("openChk", row_number.over(Window.partitionBy(col("marketTypeCode"),
        col("rdTermCode"),
        col("containerTypeCode"),
        col("paymentTermCode"),
        col("polCode"),
        col("podCode"),
        col("baseYearWeek"),
        col("xAxis")).orderBy(col("timestamp").asc)))
      .withColumn("closeChk", row_number.over(Window.partitionBy(col("marketTypeCode"),
        col("rdTermCode"),
        col("containerTypeCode"),
        col("paymentTermCode"),
        col("polCode"),
        col("podCode"),
        col("baseYearWeek"),
        col("xAxis")).orderBy(col("timestamp").desc)))
      .withColumn("open", when(col("openChk") === 1, col("dealPrice")).otherwise(lit(0))).drop("openChk")
      .withColumn("close", when(col("closeChk") === 1, col("dealPrice")).otherwise(lit(0))).drop("closeChk")
      .groupBy(col("marketTypeCode"),
        col("rdTermCode"),
        col("containerTypeCode"),
        col("paymentTermCode"),
        col("polCode"),
        col("podCode"),
        col("qtyUnit"),
        col("baseYearWeek"),
        col("interval"),
        col("xAxis"))
      .agg(max("dealPrice").as("high"),
        min("dealPrice").as("low"),
        max("open").as("open"),
        max("close").as("close"),
        sum("volume").as("volume"),
        max("timestamp").as("intervalTimestamp"))
      .withColumn("laggedPrice", lag(col("close"), 1, 0).over(Window.partitionBy(
        col("marketTypeCode"),
        col("rdTermCode"),
        col("containerTypeCode"),
        col("paymentTermCode"),
        col("polCode"),
        col("podCode"),
        col("baseYearWeek")).orderBy(col("intervalTimestamp").asc)))
      .withColumn("changeValue", col("close").minus(col("laggedPrice")))
      .withColumn("changeRate", col("changeValue").divide(col("close")))
      .distinct.drop("laggedPrice")


    lazy val src02 =
      F9S_STATS_RAW.join(FTR_DEAL, Seq("dealNumber", "dealChangeSeq"), "left")
        .withColumn("xAxis", col("timestamp").substr(lit(1), lit(6)))
        .withColumn("interval", lit("monthly")) /// 루프포인트
        .withColumn("openChk", row_number.over(Window.partitionBy(col("marketTypeCode"),
          col("rdTermCode"),
          col("containerTypeCode"),
          col("paymentTermCode"),
          col("polCode"),
          col("podCode"),
          col("baseYearWeek"),
          col("xAxis")).orderBy(col("timestamp").asc)))
        .withColumn("closeChk", row_number.over(Window.partitionBy(col("marketTypeCode"),
          col("rdTermCode"),
          col("containerTypeCode"),
          col("paymentTermCode"),
          col("polCode"),
          col("podCode"),
          col("baseYearWeek"),
          col("xAxis")).orderBy(col("timestamp").desc)))
        .withColumn("open", when(col("openChk") === 1, col("dealPrice")).otherwise(lit(0))).drop("openChk")
        .withColumn("close", when(col("closeChk") === 1, col("dealPrice")).otherwise(lit(0))).drop("closeChk")
        .groupBy(col("marketTypeCode"),
          col("rdTermCode"),
          col("containerTypeCode"),
          col("paymentTermCode"),
          col("polCode"),
          col("podCode"),
          col("qtyUnit"),
          col("baseYearWeek"),
          col("interval"),
          col("xAxis"))
        .agg(max("dealPrice").as("high"),
          min("dealPrice").as("low"),
          max("open").as("open"),
          max("close").as("close"),
          sum("volume").as("volume"),
          max("timestamp").as("intervalTimestamp"))
        .withColumn("laggedPrice", lag(col("close"), 1, 0).over(Window.partitionBy(
          col("marketTypeCode"),
          col("rdTermCode"),
          col("containerTypeCode"),
          col("paymentTermCode"),
          col("polCode"),
          col("podCode"),
          col("baseYearWeek")).orderBy(col("intervalTimestamp").asc)))
        .withColumn("changeValue", col("close").minus(col("laggedPrice")))
        .withColumn("changeRate", col("changeValue").divide(col("close")))
        .distinct.drop("laggedPrice")

    lazy val src03 =
      F9S_STATS_RAW.join(FTR_DEAL, Seq("dealNumber", "dealChangeSeq"), "left")
        .withColumn("xAxis", concat(col("timestamp").substr(lit(1), lit(4)),
          weekofyear(to_timestamp(col("timestamp").substr(lit(1), lit(8)), "yyyyMMdd"))))
        .withColumn("interval", lit("weekly")) /// 루프포인트
        .withColumn("openChk", row_number.over(Window.partitionBy(col("marketTypeCode"),
          col("rdTermCode"),
          col("containerTypeCode"),
          col("paymentTermCode"),
          col("polCode"),
          col("podCode"),
          col("baseYearWeek"),
          col("xAxis")).orderBy(col("timestamp").asc)))
        .withColumn("closeChk", row_number.over(Window.partitionBy(col("marketTypeCode"),
          col("rdTermCode"),
          col("containerTypeCode"),
          col("paymentTermCode"),
          col("polCode"),
          col("podCode"),
          col("baseYearWeek"),
          col("xAxis")).orderBy(col("timestamp").desc)))
        .withColumn("open", when(col("openChk") === 1, col("dealPrice")).otherwise(lit(0))).drop("openChk")
        .withColumn("close", when(col("closeChk") === 1, col("dealPrice")).otherwise(lit(0))).drop("closeChk")
        .groupBy(col("marketTypeCode"),
          col("rdTermCode"),
          col("containerTypeCode"),
          col("paymentTermCode"),
          col("polCode"),
          col("podCode"),
          col("qtyUnit"),
          col("baseYearWeek"),
          col("interval"),
          col("xAxis"))
        .agg(max("dealPrice").as("high"),
          min("dealPrice").as("low"),
          max("open").as("open"),
          max("close").as("close"),
          sum("volume").as("volume"),
          max("timestamp").as("intervalTimestamp"))
        .withColumn("laggedPrice", lag(col("close"), 1, 0).over(Window.partitionBy(
          col("marketTypeCode"),
          col("rdTermCode"),
          col("containerTypeCode"),
          col("paymentTermCode"),
          col("polCode"),
          col("podCode"),
          col("qtyUnit"),
          col("baseYearWeek"),
          col("interval")
        ).orderBy(col("intervalTimestamp").asc)))
        .withColumn("changeValue", col("close").minus(col("laggedPrice")))
        .withColumn("changeRate", col("changeValue").divide(col("close")))
        .distinct.drop("laggedPrice")

    val F9S_MW_WKDETAIL = src01.union(src02).union(src03).groupBy(col("marketTypeCode"),
      col("rdTermCode"),
      col("containerTypeCode"),
      col("paymentTermCode"),
      col("polCode"),
      col("podCode"),
      col("qtyUnit"),
      col("baseYearWeek"),
      col("interval"))
      .agg(collect_set(struct("xAxis", "intervalTimestamp", "open", "low", "high", "close", "volume", "changeValue", "changeRate")).as("Cell"))

    //      F9S_MW_WKDETAIL.repartition(1).drop("rte_idx","writeIdx")
    //        .write.mode("append").json(pathJsonSave+"/F9S_MW_WKDETAIL")

    F9S_MW_WKDETAIL.write.mode("append").parquet(hadoopConf.hadoopPath + "/F9S_MW_WKDETAIL")
    F9S_MW_WKDETAIL.printSchema
    //        F9S_MW_WKDETAIL.repartition(50).write.mode("append").json(pathJsonSave + "/F9S_MW_WKDETAIL")

    MongoSpark.save(F9S_MW_WKDETAIL.write
      .option("uri", mongoConf.sparkMongoUri)
      .option("database", "f9s")
      .option("collection", "F9S_MW_WKDETAIL").mode("overwrite"))
    println("/////////////////////////////JOB FINISHED//////////////////////////////")
  }

  def append_mw_wkdetail(): Unit = {
    println("////////////////////////////////MW WKDETAIL: JOB STARTED////////////////////////////////////////")

    lazy val FTR_DEAL = spark.read.parquet(hadoopConf.hadoopPath + "/FTR_DEAL")
      .select(col("DEAL_NR").as("dealNumber"),
        col("DEAL_CHNG_SEQ").as("dealChangeSeq"),
        col("TRDE_MKT_TP_CD").as("marketTypeCode"),
        col("OFER_PYMT_TRM_CD").as("paymentTermCode"),
        col("OFER_RD_TRM_CD").as("rdTermCode"),
        col("DEAL_DT").as("timestamp")
      )
    lazy val F9S_STATS_RAW = spark.read.parquet(hadoopConf.hadoopPath + "/F9S_STATS_RAW")
      .select(
        col("DEAL_NR").as("dealNumber"),
        col("DEAL_CHNG_SEQ").as("dealChangeSeq"),
        col("BSE_YW").as("baseYearWeek"),
        col("DEAL_QTY").as("volume"),
        col("DEAL_PRCE").as("dealPrice"),
        col("polCode"),
        col("podCode"))
      .withColumn("containerTypeCode", lit("01"))
      .withColumn("qtyUnit", lit("T"))
      .filter(col("dealPrice") =!= 0)

    lazy val src01 = F9S_STATS_RAW.join(FTR_DEAL, Seq("dealNumber", "dealChangeSeq"), "left")
      .withColumn("xAxis", col("timestamp").substr(lit(1), lit(8)))
      .withColumn("interval", lit("daily")) /// 루프포인트
      .withColumn("openChk", row_number.over(Window.partitionBy(col("marketTypeCode"),
        col("rdTermCode"),
        col("containerTypeCode"),
        col("paymentTermCode"),
        col("polCode"),
        col("podCode"),
        col("baseYearWeek"),
        col("xAxis")).orderBy(col("timestamp").asc)))
      .withColumn("closeChk", row_number.over(Window.partitionBy(col("marketTypeCode"),
        col("rdTermCode"),
        col("containerTypeCode"),
        col("paymentTermCode"),
        col("polCode"),
        col("podCode"),
        col("baseYearWeek"),
        col("xAxis")).orderBy(col("timestamp").desc)))
      .withColumn("open", when(col("openChk") === 1, col("dealPrice")).otherwise(lit(0))).drop("openChk")
      .withColumn("close", when(col("closeChk") === 1, col("dealPrice")).otherwise(lit(0))).drop("closeChk")
      .groupBy(col("marketTypeCode"),
        col("rdTermCode"),
        col("containerTypeCode"),
        col("paymentTermCode"),
        col("polCode"),
        col("podCode"),
        col("qtyUnit"),
        col("baseYearWeek"),
        col("interval"),
        col("xAxis"))
      .agg(max("dealPrice").as("high"),
        min("dealPrice").as("low"),
        max("open").as("open"),
        max("close").as("close"),
        sum("volume").as("volume"),
        max("timestamp").as("intervalTimestamp"))
      .withColumn("laggedPrice", lag(col("close"), 1, 0).over(Window.partitionBy(
        col("marketTypeCode"),
        col("rdTermCode"),
        col("containerTypeCode"),
        col("paymentTermCode"),
        col("polCode"),
        col("podCode"),
        col("baseYearWeek")).orderBy(col("intervalTimestamp").asc)))
      .withColumn("changeValue", col("close").minus(col("laggedPrice")))
      .withColumn("changeRate", col("changeValue").divide(col("close")))
      .distinct.drop("laggedPrice")


    lazy val src02 =
      F9S_STATS_RAW.join(FTR_DEAL, Seq("dealNumber", "dealChangeSeq"), "left")
        .withColumn("xAxis", col("timestamp").substr(lit(1), lit(6)))
        .withColumn("interval", lit("monthly")) /// 루프포인트
        .withColumn("openChk", row_number.over(Window.partitionBy(col("marketTypeCode"),
          col("rdTermCode"),
          col("containerTypeCode"),
          col("paymentTermCode"),
          col("polCode"),
          col("podCode"),
          col("baseYearWeek"),
          col("xAxis")).orderBy(col("timestamp").asc)))
        .withColumn("closeChk", row_number.over(Window.partitionBy(col("marketTypeCode"),
          col("rdTermCode"),
          col("containerTypeCode"),
          col("paymentTermCode"),
          col("polCode"),
          col("podCode"),
          col("baseYearWeek"),
          col("xAxis")).orderBy(col("timestamp").desc)))
        .withColumn("open", when(col("openChk") === 1, col("dealPrice")).otherwise(lit(0))).drop("openChk")
        .withColumn("close", when(col("closeChk") === 1, col("dealPrice")).otherwise(lit(0))).drop("closeChk")
        .groupBy(col("marketTypeCode"),
          col("rdTermCode"),
          col("containerTypeCode"),
          col("paymentTermCode"),
          col("polCode"),
          col("podCode"),
          col("qtyUnit"),
          col("baseYearWeek"),
          col("interval"),
          col("xAxis"))
        .agg(max("dealPrice").as("high"),
          min("dealPrice").as("low"),
          max("open").as("open"),
          max("close").as("close"),
          sum("volume").as("volume"),
          max("timestamp").as("intervalTimestamp"))
        .withColumn("laggedPrice", lag(col("close"), 1, 0).over(Window.partitionBy(
          col("marketTypeCode"),
          col("rdTermCode"),
          col("containerTypeCode"),
          col("paymentTermCode"),
          col("polCode"),
          col("podCode"),
          col("baseYearWeek")).orderBy(col("intervalTimestamp").asc)))
        .withColumn("changeValue", col("close").minus(col("laggedPrice")))
        .withColumn("changeRate", col("changeValue").divide(col("close")))
        .distinct.drop("laggedPrice")

    lazy val src03 =
      F9S_STATS_RAW.join(FTR_DEAL, Seq("dealNumber", "dealChangeSeq"), "left")
        .withColumn("xAxis", concat(col("timestamp").substr(lit(1), lit(4)),
          weekofyear(to_timestamp(col("timestamp").substr(lit(1), lit(8)), "yyyyMMdd"))))
        .withColumn("interval", lit("weekly")) /// 루프포인트
        .withColumn("openChk", row_number.over(Window.partitionBy(col("marketTypeCode"),
          col("rdTermCode"),
          col("containerTypeCode"),
          col("paymentTermCode"),
          col("polCode"),
          col("podCode"),
          col("baseYearWeek"),
          col("xAxis")).orderBy(col("timestamp").asc)))
        .withColumn("closeChk", row_number.over(Window.partitionBy(col("marketTypeCode"),
          col("rdTermCode"),
          col("containerTypeCode"),
          col("paymentTermCode"),
          col("polCode"),
          col("podCode"),
          col("baseYearWeek"),
          col("xAxis")).orderBy(col("timestamp").desc)))
        .withColumn("open", when(col("openChk") === 1, col("dealPrice")).otherwise(lit(0))).drop("openChk")
        .withColumn("close", when(col("closeChk") === 1, col("dealPrice")).otherwise(lit(0))).drop("closeChk")
        .groupBy(col("marketTypeCode"),
          col("rdTermCode"),
          col("containerTypeCode"),
          col("paymentTermCode"),
          col("polCode"),
          col("podCode"),
          col("qtyUnit"),
          col("baseYearWeek"),
          col("interval"),
          col("xAxis"))
        .agg(max("dealPrice").as("high"),
          min("dealPrice").as("low"),
          max("open").as("open"),
          max("close").as("close"),
          sum("volume").as("volume"),
          max("timestamp").as("intervalTimestamp"))
        .withColumn("laggedPrice", lag(col("close"), 1, 0).over(Window.partitionBy(
          col("marketTypeCode"),
          col("rdTermCode"),
          col("containerTypeCode"),
          col("paymentTermCode"),
          col("polCode"),
          col("podCode"),
          col("qtyUnit"),
          col("baseYearWeek"),
          col("interval")
        ).orderBy(col("intervalTimestamp").asc)))
        .withColumn("changeValue", col("close").minus(col("laggedPrice")))
        .withColumn("changeRate", col("changeValue").divide(col("close")))
        .distinct.drop("laggedPrice")

    val F9S_MW_WKDETAIL = src01.union(src02).union(src03).groupBy(col("marketTypeCode"),
      col("rdTermCode"),
      col("containerTypeCode"),
      col("paymentTermCode"),
      col("polCode"),
      col("podCode"),
      col("qtyUnit"),
      col("baseYearWeek"),
      col("interval"))
      .agg(collect_set(struct("xAxis", "intervalTimestamp", "open", "low", "high", "close", "volume", "changeValue", "changeRate")).as("Cell"))

    //      F9S_MW_WKDETAIL.repartition(1).drop("rte_idx","writeIdx")
    //        .write.mode("append").json(pathJsonSave+"/F9S_MW_WKDETAIL")

    F9S_MW_WKDETAIL.write.mode("append").parquet(hadoopConf.hadoopPath + "/F9S_MW_WKDETAIL")
    F9S_MW_WKDETAIL.printSchema
    //        F9S_MW_WKDETAIL.repartition(50).write.mode("append").json(pathJsonSave + "/F9S_MW_WKDETAIL")

    MongoSpark.save(F9S_MW_WKDETAIL.write
      .option("uri", mongoConf.sparkMongoUri)
      .option("database", "f9s")
      .option("collection", "F9S_MW_WKDETAIL").mode("append"))
    println("/////////////////////////////JOB FINISHED//////////////////////////////")
  }
}

