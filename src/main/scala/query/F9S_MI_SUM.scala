package query

import org.apache.parquet.format.IntType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}

case class F9S_MI_SUM(var spark: SparkSession, var pathSourceFrom: String,
                      var pathParquetSave: String, var pathJsonSave: String){
  def mi_sum(): Unit ={
    val SCFI = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(pathSourceFrom+"/SCFI.csv")

    val step1 = SCFI.drop("share", "qtyUnit", "currency")
      .withColumn("year", year(col("date")))
      .withColumn("month", year(col("date"))*100+month(col("date")))
      .withColumn("week", year(col("date"))*100+weekofyear(col("date"))).drop("date").distinct
    step1.printSchema()

    val apnd1= step1.groupBy("idxSubject", "idxCategory", "idxCd", "idxNm", "week").agg(avg("value").as("value"))
      .withColumn("interval", lit("week"+"ly")) // 루프 포인트
      .withColumn("tmp", lag(col("value"), 1, 0).over(Window.partitionBy(col("idxNm")).orderBy(col("week").asc)))
      .withColumn("changeValue", col("value")-col("tmp")).drop("tmp")
      .withColumn("changeRate", col("changeValue")/col("value"))
      .withColumn("intervalStamp", col("week").cast(StringType)).drop("week")
      .withColumn("volume", lit(0).cast(DoubleType)).distinct
    val apnd2= step1.groupBy("idxSubject", "idxCategory", "idxCd", "idxNm", "year").agg(avg("value").as("value"))
      .withColumn("interval", lit("year"+"ly")) // 루프 포인트
      .withColumn("tmp", lag(col("value"), 1, 0).over(Window.partitionBy(col("idxNm")).orderBy(col("year").asc)))
      .withColumn("changeValue", col("value")-col("tmp")).drop("tmp")
      .withColumn("changeRate", col("changeValue")/col("value"))
      .withColumn("intervalStamp", col("year").cast(StringType)).drop("year")
      .withColumn("volume", lit(0).cast(DoubleType)).distinct
    val apnd3= step1.groupBy("idxSubject", "idxCategory", "idxCd", "idxNm", "month").agg(avg("value").as("value"))
      .withColumn("interval", lit("month"+"ly")) // 루프 포인트
      .withColumn("tmp", lag(col("value"), 1, 0).over(Window.partitionBy(col("idxNm")).orderBy(col("month").asc)))
      .withColumn("changeValue", col("value")-col("tmp")).drop("tmp")
      .withColumn("changeRate", col("changeValue")/col("value"))
      .withColumn("intervalStamp", col("month").cast(StringType)).drop("month")
      .withColumn("volume", lit(0).cast(DoubleType)).distinct

    val finalSrc = apnd1.union(apnd2).union(apnd3)

    val F9S_MI_SUM = finalSrc.groupBy("idxSubject", "idxCategory", "idxCd", "idxNm", "interval").agg(collect_list(struct("intervalStamp", "value", "changeValue", "changeRate", "volume")).as("Cell"))

    F9S_MI_SUM.printSchema
    lazy val mwidx = F9S_MI_SUM.select("idxSubject", "idxCategory", "idxCd", "interval").distinct
    lazy val idxSubject = mwidx.select("idxSubject").collect().map(_(0).toString)
    lazy val idxCategory = mwidx.select("idxCategory").collect().map(_(0).toString)
    lazy val idxCd = mwidx.select("idxCd").collect().map(_(0).toString)
    lazy val idx = mwidx.collect()
    lazy val interval = mwidx.select("interval").collect().map(_(0).toString)

    lazy val tgData = F9S_MI_SUM

    for (i <- idx.indices){
      tgData.filter(
        col("idxSubject") === idxSubject(i) &&
          col("idxCategory") === idxCategory(i) &&
          col("idxCd") === idxCd(i) &&
          col("interval") === interval(i)
      )
        .write.mode("append").json(pathJsonSave+"/F9S_MI_SUM"+"/"+idxSubject(i)+"/"+idxCategory(i)+ "/" + idxCd(i)+"/"+interval(i))
    }

    F9S_MI_SUM.write.mode("append").parquet(pathParquetSave+"/F9S_MI_SUM")

  }
}
