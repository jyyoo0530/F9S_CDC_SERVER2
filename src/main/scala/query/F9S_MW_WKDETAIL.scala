package query

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

case class F9S_MW_WKDETAIL(var spark: SparkSession, var pathSourceFrom: String,
                           var pathParquetSave: String, var pathJsonSave: String){
  def mw_wkdetail(): Unit ={
    lazy val aggData = spark.read.parquet(pathParquetSave+"/aggData")


    val wkDetailData = aggData.withColumn("year", col("timestamp").substr(lit(1), lit(4)))
      .withColumn("month", col("timestamp").substr(lit(1), lit(6)))
      .withColumn("day", col("timestamp").substr(lit(1), lit(8)))
      .withColumn("week", weekofyear(to_timestamp(col("timestamp").substr(lit(1),lit(14)), "yyyyMMddhhmmss")))
      .withColumn("interval", lit("daily")) /// 루프포인트
      .withColumn("intervalTimestamp", col("day")) /// 루프포인트
      .distinct

    lazy val openPrce = wkDetailData.withColumn("numerator", row_number.over(Window.partitionBy(col("marketTypeCode"), col("rdTermCode"), col("containerTypeCode"), col("paymentTermCode"), col("polCode"), col("podCode"), col("qtyUnit"),col("baseYearWeek"),col("intervalTimestamp"), col("interval")).orderBy(col("timestamp").asc)))
      .filter(col("numerator") === 1)
      .withColumn("open", col("dealPrice"))
      .select("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit","baseYearWeek","intervalTimestamp", "interval", "open")

    lazy val closePrce = wkDetailData.withColumn("numerator", row_number.over(Window.partitionBy(col("marketTypeCode"), col("rdTermCode"), col("containerTypeCode"), col("paymentTermCode"), col("polCode"), col("podCode"), col("qtyUnit"),col("baseYearWeek"),col("intervalTimestamp"), col("interval")).orderBy(col("timestamp").desc)))
      .filter(col("numerator") === 1)
      .withColumn("close", col("dealPrice"))
      .select("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit","baseYearWeek","intervalTimestamp", "interval", "close")

    lazy val changeValue = openPrce.join(closePrce, Seq("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit","baseYearWeek","intervalTimestamp", "interval"), "left")
      .withColumn("tmp", lag(col("close"), 1, 0).over(Window.partitionBy(col("marketTypeCode"), col("rdTermCode"), col("containerTypeCode"), col("paymentTermCode"), col("polCode"), col("podCode"), col("qtyUnit"),col("baseYearWeek"),col("intervalTimestamp"), col("interval")).orderBy(col("polCode").asc, col("podCode").asc, col("intervalTimestamp").asc)))
      .withColumn("changeValue", col("close")-col("tmp")).drop("tmp")
    lazy val changeRate = changeValue.withColumn("changeRate", col("changeValue")/col("close"))


    val F9S_MW_WKDETAIL = wkDetailData.groupBy("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit","baseYearWeek","intervalTimestamp", "interval")
      .agg(max("dealPrice").as("high"), min("dealPrice").as("low"), sum("dealQty").as("volume"))
      .join(changeRate, Seq("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit","baseYearWeek","intervalTimestamp", "interval"), "left")
      .groupBy("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "qtyUnit","baseYearWeek", "interval")
      .agg(collect_list(struct("intervalTimestamp","open","high","low","close","changeValue","changeRate","volume")).as("Cell"))
      .drop("intervalTimestamp","open","high","low","close","changeValue","changeRate","volume")

    lazy val mwidx = F9S_MW_WKDETAIL.select("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "baseYearWeek","interval").distinct.withColumn("writeIdx", concat(col("marketTypeCode"), col("rdTermCode"), col("containerTypeCode"), col("paymentTermCode"), col("polCode"), col("podCode"), col("baseYearWeek"), col("interval"))).withColumn("idx", row_number.over(Window.orderBy(col("writeIdx")))).withColumn("rte_idx", concat(col("polCode"), col("podCode")))
    lazy val idx = mwidx.collect()
    lazy val marketTypeCode = mwidx.select("marketTypeCode").collect().map(_(0).toString)
    lazy val paymentTermCode = mwidx.select("paymentTermCode").collect().map(_(0).toString)
    lazy val rdTermCode = mwidx.select("rdTermCode").collect().map(_(0).toString)
    lazy val containerTypeCode = mwidx.select("containerTypeCode").collect().map(_(0).toString)
    lazy val rte_idx = mwidx.select("rte_idx").collect().map(_(0).toString)
    lazy val baseYearWeek = mwidx.select("baseYearWeek").collect().map(_(0).toString)
    lazy val interval = mwidx.select("interval").collect().map(_(0).toString)

    lazy val tgData = F9S_MW_WKDETAIL.withColumn("writeIdx", concat(col("marketTypeCode"), col("baseYearWeek"), col("rdTermCode"), col("containerTypeCode"), col("paymentTermCode"), col("polCode"), col("podCode"), col("interval"))).withColumn("rte_idx", concat(col("polCode"), col("podCode")))

    for (i <- idx.indices){
      tgData.filter(
        col("marketTypeCode") === marketTypeCode(i) &&
          col("paymentTermCode") === paymentTermCode(i) &&
          col("rdTermCode") === rdTermCode(i) &&
          col("containerTypeCode") === containerTypeCode(i) &&
          col("rte_idx") === rte_idx(i) &&
          col("baseYearWeek") === baseYearWeek(i) &&
          col("interval") === interval(i)
      )
        .drop("rte_idx","writeIdx")
        .write.mode("append").json(pathJsonSave+"/F9S_MW_WKDETAIL"+"/"+marketTypeCode(i)+ "/" + baseYearWeek(i)+"/"+paymentTermCode(i)+"/"+rdTermCode(i)+"/"+containerTypeCode(i)+"/"+rte_idx(i) +"/"+interval(i))
    }

    F9S_MW_WKDETAIL.write.mode("append").parquet(pathParquetSave+"/F9S_MW_WKDETAIL")

  }


}
