package f9s.core.query

import com.mongodb.spark.MongoSpark
import f9s.{appConf, hadoopConf, mongoConf}
import org.apache.parquet.format.IntType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}

case class F9S_MI_SUM(var spark: SparkSession) {

  val filePath = appConf().dataLake match {
    case "file" => appConf().folderOrigin
    case "hadoop" => hadoopConf.hadoopPath
  }

  def mi_sum(): DataFrame = {
    println("////////////////////////////////MI SUM: JOB STARTED////////////////////////////////////////")
    ///// DATA LOAD /////
    val SCFI = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(filePath + "/SCFI.csv")
    lazy val weektable = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(filePath + "/weektable.csv")
      .select(col("week2").cast(StringType), col("yyyymmdd").cast(StringType))
      .withColumn("week", col("week2"))
      .withColumn("intervalTimestamp", concat(col("yyyymmdd"), lit("010000000000")))
      .drop("yyyymmdd", "week2")


    ///// SQL /////
    val step1 = SCFI.drop("share", "qtyUnit", "currency")
      .withColumn("year", concat(year(col("date").cast(StringType)), lit("1220010000000000")))
      .withColumn("month", concat((year(col("date")) * 100 + month(col("date"))).cast(StringType), lit("20010000000000")))
      .withColumn("week", concat(year(col("date")).cast(StringType), weekofyear(col("date"))).cast(StringType)).drop("date").distinct
    step1.printSchema()

    val apnd1 = step1.groupBy("idxSubject", "idxCategory", "idxCd", "idxNm", "week").agg(avg("value").as("value"))
      .withColumn("xAxis", col("week"))
      .withColumn("interval", lit("week" + "ly")) // 루프 포인트
      .withColumn("tmp", lag(col("value"), 1, 0).over(Window.partitionBy(col("idxNm")).orderBy(col("week").asc)))
      .withColumn("changeValue", col("value") - col("tmp")).drop("tmp")
      .withColumn("changeRate", col("changeValue") / col("value"))
      .join(weektable, Seq("week"), "left")
      .withColumn("volume", lit(0).cast(DoubleType))
      .drop("week").distinct

    val apnd2 = step1.groupBy("idxSubject", "idxCategory", "idxCd", "idxNm", "year").agg(avg("value").as("value"))
      .withColumn("xAxis", col("year").substr(lit(1), lit(4)))
      .withColumn("interval", lit("year" + "ly")) // 루프 포인트
      .withColumn("tmp", lag(col("value"), 1, 0).over(Window.partitionBy(col("idxNm")).orderBy(col("year").asc)))
      .withColumn("changeValue", col("value") - col("tmp")).drop("tmp")
      .withColumn("changeRate", col("changeValue") / col("value"))
      .withColumn("intervalTimestamp", col("year").cast(StringType))
      .withColumn("volume", lit(0).cast(DoubleType))
      .drop("year").distinct

    val apnd3 = step1.groupBy("idxSubject", "idxCategory", "idxCd", "idxNm", "month").agg(avg("value").as("value"))
      .withColumn("xAxis", col("month").substr(lit(1), lit(6)))
      .withColumn("interval", lit("month" + "ly")) // 루프 포인트
      .withColumn("tmp", lag(col("value"), 1, 0).over(Window.partitionBy(col("idxNm")).orderBy(col("month").asc)))
      .withColumn("changeValue", col("value") - col("tmp")).drop("tmp")
      .withColumn("changeRate", col("changeValue") / col("value"))
      .withColumn("intervalTimestamp", col("month").cast(StringType))
      .withColumn("volume", lit(0).cast(DoubleType))
      .drop("month").distinct

    val finalSrc = apnd1.union(apnd2).union(apnd3)

    val F9S_MI_SUM = finalSrc.groupBy("idxSubject", "idxCategory", "idxCd", "idxNm", "interval").agg(collect_list(struct(
      col("xAxis").cast(StringType).as("xAxis"),
      col("intervalTimestamp").cast(StringType).as("intervalTimestamp"),
      col("value").cast(DoubleType).as("value"),
      col("changeValue").cast(DoubleType).as("changeValue"),
      col("changeRate").cast(DoubleType).as("changeRate"),
      col("volume").cast(DoubleType).as("volume")
    )).as("Cell"))


    ///// DATALAKE -> 2nd Tier /////
    F9S_MI_SUM.printSchema
    println("/////////////////////////////JOB FINISHED//////////////////////////////")

    F9S_MI_SUM
  }

  def append_mi_sum(): Unit = {
    println("////////////////////////////////MI SUM: JOB STARTED////////////////////////////////////////")
    val SCFI = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(filePath + "/SCFI.csv")

    lazy val weektable = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(filePath + "/weektable.csv")
      .select(col("week2").cast(StringType), col("yyyymmdd").cast(StringType))
      .withColumn("week", col("week2"))
      .withColumn("intervalTimestamp", concat(col("yyyymmdd"), lit("010000000000")))
      .drop("yyyymmdd", "week2")

    val step1 = SCFI.drop("share", "qtyUnit", "currency")
      .withColumn("year", concat(year(col("date").cast(StringType)), lit("1220010000000000")))
      .withColumn("month", concat((year(col("date")) * 100 + month(col("date"))).cast(StringType), lit("20010000000000")))
      .withColumn("week", concat(year(col("date")).cast(StringType), weekofyear(col("date"))).cast(StringType)).drop("date").distinct
    step1.printSchema()

    val apnd1 = step1.groupBy("idxSubject", "idxCategory", "idxCd", "idxNm", "week").agg(avg("value").as("value"))
      .withColumn("xAxis", col("week"))
      .withColumn("interval", lit("week" + "ly")) // 루프 포인트
      .withColumn("tmp", lag(col("value"), 1, 0).over(Window.partitionBy(col("idxNm")).orderBy(col("week").asc)))
      .withColumn("changeValue", col("value") - col("tmp")).drop("tmp")
      .withColumn("changeRate", col("changeValue") / col("value"))
      .join(weektable, Seq("week"), "left")
      .withColumn("volume", lit(0).cast(DoubleType))
      .drop("week").distinct

    val apnd2 = step1.groupBy("idxSubject", "idxCategory", "idxCd", "idxNm", "year").agg(avg("value").as("value"))
      .withColumn("xAxis", col("year"))
      .withColumn("interval", lit("year" + "ly")) // 루프 포인트
      .withColumn("tmp", lag(col("value"), 1, 0).over(Window.partitionBy(col("idxNm")).orderBy(col("year").asc)))
      .withColumn("changeValue", col("value") - col("tmp")).drop("tmp")
      .withColumn("changeRate", col("changeValue") / col("value"))
      .withColumn("intervalTimestamp", col("year").cast(StringType))
      .withColumn("volume", lit(0).cast(DoubleType))
      .drop("year").distinct

    val apnd3 = step1.groupBy("idxSubject", "idxCategory", "idxCd", "idxNm", "month").agg(avg("value").as("value"))
      .withColumn("xAxis", col("month"))
      .withColumn("interval", lit("month" + "ly")) // 루프 포인트
      .withColumn("tmp", lag(col("value"), 1, 0).over(Window.partitionBy(col("idxNm")).orderBy(col("month").asc)))
      .withColumn("changeValue", col("value") - col("tmp")).drop("tmp")
      .withColumn("changeRate", col("changeValue") / col("value"))
      .withColumn("intervalTimestamp", col("month").cast(StringType))
      .withColumn("volume", lit(0).cast(DoubleType))
      .drop("month").distinct

    apnd1.printSchema()
    apnd2.printSchema()
    apnd3.printSchema()

    val finalSrc = apnd1.union(apnd2).union(apnd3)

    val F9S_MI_SUM = finalSrc.groupBy("idxSubject", "idxCategory", "idxCd", "idxNm", "interval").agg(collect_list(struct(
      col("xAxis").cast(StringType),
      col("intervalTimestamp").cast(StringType),
      col("value").cast(DoubleType),
      col("changeValue").cast(DoubleType),
      col("changeRate").cast(DoubleType),
      col("volume").cast(DoubleType))).as("Cell"))

    //    F9S_MI_SUM.repartition(1).write.mode("append").json(pathJsonSave + "/F9S_MI_SUM")

    F9S_MI_SUM.write.mode("append").parquet(filePath + "/F9S_MI_SUM")
    MongoSpark.save(F9S_MI_SUM.write
      .option("uri", mongoConf.sparkMongoUri)
      .option("database", "f9s")
      .option("collection", "F9S_MI_SUM").mode("append"))
    F9S_MI_SUM.printSchema
    println("/////////////////////////////JOB FINISHED//////////////////////////////")
  }
}