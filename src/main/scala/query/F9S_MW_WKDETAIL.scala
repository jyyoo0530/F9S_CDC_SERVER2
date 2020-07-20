package query

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

case class F9S_MW_WKDETAIL(var spark: SparkSession, var pathSourceFrom: String,
                           var pathParquetSave: String, var pathJsonSave: String) {
  def mw_wkdetail(): Unit = {
    println("////////////////////////////////MW WKDETAIL: JOB STARTED////////////////////////////////////////")
    lazy val FTR_DEAL = spark.read.parquet(pathSourceFrom + "/FTR_DEAL")
      .select(col("DEAL_NR").as("dealNumber"),
        col("DEAL_CHNG_SEQ").as("dealChangeSeq"),
        col("TRDE_MKT_TP_CD").as("marketTypeCode"),
        col("OFER_PYMT_TRM_CD").as("paymentTermCode"),
        col("OFER_RD_TRM_CD").as("rdTermCode"),
        col("DEAL_DT").as("timestamp")
      )
    lazy val F9S_STATS_RAW = spark.read.parquet(pathParquetSave + "/F9S_STATS_RAW")
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


    val src = F9S_STATS_RAW.join(FTR_DEAL, Seq("dealNumber", "dealChangeSeq"), "left")
      .withColumn("day", col("timestamp").substr(lit(1), lit(8)))
      .withColumn("interval", lit("daily")) /// 루프포인트
      .withColumn("openChk", row_number.over(Window.partitionBy(col("marketTypeCode"),
        col("rdTermCode"),
        col("containerTypeCode"),
        col("paymentTermCode"),
        col("polCode"),
        col("podCode"),
        col("qtyUnit"),
        col("baseYearWeek"),
        col("day")).orderBy(col("timestamp").asc)))
      .withColumn("closeChk", row_number.over(Window.partitionBy(col("marketTypeCode"),
        col("rdTermCode"),
        col("containerTypeCode"),
        col("paymentTermCode"),
        col("polCode"),
        col("podCode"),
        col("qtyUnit"),
        col("baseYearWeek"),
        col("day")).orderBy(col("timestamp").desc)))
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
        col("day"))
      .agg(max("dealPrice").as("high"),
        min("dealPrice").as("low"),
        max("open").as("open"),
        max("close").as("close"),
        sum("volume").as("volume"))
      .withColumn("intervalTimestamp", concat(col("day"), lit("233000000000")))
      .withColumn("laggedPrice", lag(col("close"), 1, 0).over(Window.partitionBy(
        col("marketTypeCode"),
        col("rdTermCode"),
        col("containerTypeCode"),
        col("paymentTermCode"),
        col("polCode"),
        col("podCode"),
        col("qtyUnit"),
        col("baseYearWeek"),
        col("interval"),
        col("day")
      ).orderBy(col("day").asc)))
      .withColumn("changeValue", col("close").minus(col("laggedPrice")))
      .withColumn("changeRate", col("changeValue").divide(col("close"))).drop("laggedPrice", "day")
      .distinct
      .union(
        F9S_STATS_RAW.join(FTR_DEAL, Seq("dealNumber", "dealChangeSeq"), "left")
          .withColumn("month", col("timestamp").substr(lit(1), lit(6)))
          .withColumn("interval", lit("monthly")) /// 루프포인트
          .withColumn("openChk", row_number.over(Window.partitionBy(col("marketTypeCode"),
            col("rdTermCode"),
            col("containerTypeCode"),
            col("paymentTermCode"),
            col("polCode"),
            col("podCode"),
            col("qtyUnit"),
            col("baseYearWeek"),
            col("month")).orderBy(col("timestamp").asc)))
          .withColumn("closeChk", row_number.over(Window.partitionBy(col("marketTypeCode"),
            col("rdTermCode"),
            col("containerTypeCode"),
            col("paymentTermCode"),
            col("polCode"),
            col("podCode"),
            col("qtyUnit"),
            col("baseYearWeek"),
            col("month")).orderBy(col("timestamp").desc)))
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
            col("month"))
          .agg(max("dealPrice").as("high"),
            min("dealPrice").as("low"),
            max("open").as("open"),
            max("close").as("close"),
            sum("volume").as("volume"))
          .withColumn("intervalTimestamp", concat(col("month"), lit("25000000000000")))
          .withColumn("laggedPrice", lag(col("close"), 1, 0).over(Window.partitionBy(
            col("marketTypeCode"),
            col("rdTermCode"),
            col("containerTypeCode"),
            col("paymentTermCode"),
            col("polCode"),
            col("podCode"),
            col("qtyUnit"),
            col("baseYearWeek"),
            col("interval"),
            col("month")
          ).orderBy(col("month").asc)))
          .withColumn("changeValue", col("close").minus(col("laggedPrice")))
          .withColumn("changeRate", col("changeValue").divide(col("close"))).drop("laggedPrice", "month")
          .distinct
      )
      .union(
        F9S_STATS_RAW.join(FTR_DEAL, Seq("dealNumber", "dealChangeSeq"), "left")
          .withColumn("year", col("timestamp").substr(lit(1), lit(4)))
          .withColumn("interval", lit("year")) /// 루프포인트
          .withColumn("openChk", row_number.over(Window.partitionBy(col("marketTypeCode"),
            col("rdTermCode"),
            col("containerTypeCode"),
            col("paymentTermCode"),
            col("polCode"),
            col("podCode"),
            col("qtyUnit"),
            col("baseYearWeek"),
            col("year")).orderBy(col("timestamp").asc)))
          .withColumn("closeChk", row_number.over(Window.partitionBy(col("marketTypeCode"),
            col("rdTermCode"),
            col("containerTypeCode"),
            col("paymentTermCode"),
            col("polCode"),
            col("podCode"),
            col("qtyUnit"),
            col("baseYearWeek"),
            col("year")).orderBy(col("timestamp").desc)))
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
            col("year"))
          .agg(max("dealPrice").as("high"),
            min("dealPrice").as("low"),
            max("open").as("open"),
            max("close").as("close"),
            sum("volume").as("volume"))
          .withColumn("intervalTimestamp", concat(col("year"), lit("1215000000000000")))
          .withColumn("laggedPrice", lag(col("close"), 1, 0).over(Window.partitionBy(
            col("marketTypeCode"),
            col("rdTermCode"),
            col("containerTypeCode"),
            col("paymentTermCode"),
            col("polCode"),
            col("podCode"),
            col("qtyUnit"),
            col("baseYearWeek"),
            col("interval"),
            col("year")
          ).orderBy(col("year").asc)))
          .withColumn("changeValue", col("close").minus(col("laggedPrice")))
          .withColumn("changeRate", col("changeValue").divide(col("close"))).drop("laggedPrice", "year")
          .distinct
      )


    val F9S_MW_WKDETAIL = src.groupBy(col("marketTypeCode"),
      col("rdTermCode"),
      col("containerTypeCode"),
      col("paymentTermCode"),
      col("polCode"),
      col("podCode"),
      col("qtyUnit"),
      col("baseYearWeek"),
      col("interval")
    )
      .agg(collect_set(struct("intervalTimestamp", "open", "low", "high", "close", "volume", "changeValue", "changeRate")).as("Cell"))

    //      F9S_MW_WKDETAIL.repartition(1).drop("rte_idx","writeIdx")
    //        .write.mode("append").json(pathJsonSave+"/F9S_MW_WKDETAIL")

    //    F9S_MW_WKDETAIL.write.mode("overwrite").parquet(pathParquetSave + "/F9S_MW_WKDETAIL")
    F9S_MW_WKDETAIL.printSchema
    F9S_MW_WKDETAIL.write.mode("append").json(pathJsonSave + "/F9S_MW_WKDETAIL")

    MongoSpark.save(F9S_MW_WKDETAIL.write
      .option("uri", "mongodb://data.freight9.com/f9s")
      .option("collection", "F9S_MW_WKDETAIL").mode("overwrite"))
    println("/////////////////////////////JOB FINISHED//////////////////////////////")
  }


}
