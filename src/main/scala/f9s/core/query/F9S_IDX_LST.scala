package f9s.core.query

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class F9S_IDX_LST(var spark: SparkSession, var pathParquetSave: String, var pathJsonSave: String) {
  def idx_lst(): Unit = {
    println("////////////////////////////////IDX LST: JOB STARTED////////////////////////////////////////")
    val F9S_MI_SUM = spark.read.parquet(pathParquetSave + "/F9S_MI_SUM")
    val F9S_MW_WKDETAIL = spark.read.parquet(pathParquetSave + "/F9S_MW_WKDETAIL").select("interval")
    val schema = StructType(List(
      StructField("intervalSeq", IntegerType, nullable = false),
      StructField("interval", StringType, nullable = false)
    ))
    val rdd = spark.sparkContext.parallelize(Seq(
      Row(1, "tick"),
      Row(2, "secondly"),
      Row(3, "10sec"),
      Row(4, "30sec"),
      Row(5, "minutely"),
      Row(6, "5min"),
      Row(7, "10min"),
      Row(8, "30min"),
      Row(9, "hourly"),
      Row(10, "daily"),
      Row(11, "weekly"),
      Row(12, "monthly"),
      Row(13, "yearly")
    ))
    val intervalDf = spark.createDataFrame(rdd, schema)

    val F9S_IDX_LST = F9S_MI_SUM
      .join(intervalDf, Seq("interval"), "left")
      .sort(col("intervalSeq").asc).distinct
      .groupBy("idxSubject", "idxCategory", "idxCd", "idxNm")
      .agg(collect_list(struct("intervalSeq", "interval")).as("intervalItem"))
      .union(
        F9S_MW_WKDETAIL.join(intervalDf, Seq("interval"), "left")
          .distinct
          .withColumn("idxSubject", lit("marketWatch"))
          .withColumn("idxCategory", lit("weekDetail"))
          .withColumn("idxCd", lit("MWWD"))
          .withColumn("idxNm", lit("market watch weekdetail frequencies"))
          .groupBy("idxSubject", "idxCategory", "idxCd", "idxNm")
          .agg(collect_set(struct("intervalSeq", "interval")).as("intervalItem"))
      )


    //    F9S_IDX_LST.repartition(1).write.mode("append").json(pathJsonSave + "/F9S_IDX_LST")
    //        F9S_IDX_LST.write.mode("overwrite").parquet(pathParquetSave+"/F9S_IDX_LST")
    MongoSpark.save(F9S_IDX_LST.write
      .option("uri", "mongodb://ec2-13-209-15-68.ap-northeast-2.compute.amazonaws.com:27017/f9s")
      .option("collection", "F9S_IDX_LST").mode("overwrite"))
    F9S_IDX_LST.printSchema
    println("/////////////////////////////JOB FINISHED//////////////////////////////")
  }

  def append_idx_lst(): Unit = {
    println("////////////////////////////////IDX LST: JOB STARTED////////////////////////////////////////")
    val F9S_MI_SUM = spark.read.parquet(pathParquetSave + "/F9S_MI_SUM")
    val F9S_MW_WKDETAIL = spark.read.parquet(pathParquetSave + "/F9S_MW_WKDETAIL").select("interval")
    val schema = StructType(List(
      StructField("intervalSeq", IntegerType, nullable = false),
      StructField("interval", StringType, nullable = false)
    ))
    val rdd = spark.sparkContext.parallelize(Seq(
      Row(1, "tick"),
      Row(2, "secondly"),
      Row(3, "10sec"),
      Row(4, "30sec"),
      Row(5, "minutely"),
      Row(6, "5min"),
      Row(7, "10min"),
      Row(8, "30min"),
      Row(9, "hourly"),
      Row(10, "daily"),
      Row(11, "weekly"),
      Row(12, "monthly"),
      Row(13, "yearly")
    ))
    val intervalDf = spark.createDataFrame(rdd, schema)

    val F9S_IDX_LST = F9S_MI_SUM
      .join(intervalDf, Seq("interval"), "left")
      .sort(col("intervalSeq").asc).distinct
      .groupBy("idxSubject", "idxCategory", "idxCd", "idxNm")
      .agg(collect_list(struct("intervalSeq", "interval")).as("intervalItem"))
      .union(
        F9S_MW_WKDETAIL.join(intervalDf, Seq("interval"), "left")
          .distinct
          .withColumn("idxSubject", lit("marketWatch"))
          .withColumn("idxCategory", lit("weekDetail"))
          .withColumn("idxCd", lit("MWWD"))
          .withColumn("idxNm", lit("market watch weekdetail frequencies"))
          .groupBy("idxSubject", "idxCategory", "idxCd", "idxNm")
          .agg(collect_set(struct("intervalSeq", "interval")).as("intervalItem"))
      )


    //    F9S_IDX_LST.repartition(1).write.mode("append").json(pathJsonSave + "/F9S_IDX_LST")
    //        F9S_IDX_LST.write.mode("overwrite").parquet(pathParquetSave+"/F9S_IDX_LST")
    MongoSpark.save(F9S_IDX_LST.write
      .option("uri", "mongodb://ec2-13-209-15-68.ap-northeast-2.compute.amazonaws.com:27017/f9s")
      .option("collection", "F9S_IDX_LST").mode("overwrite"))
    F9S_IDX_LST.printSchema
    println("/////////////////////////////JOB FINISHED//////////////////////////////")
  }
}
