package query

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class F9S_CRYR_LST(var spark: SparkSession, var pathSourceFrom: String,
                        var pathParquetSave: String, var pathJsonSave: String){
  def cryr_lst(): Unit ={
    lazy val FTR_OFER_CRYR = spark.read.parquet(pathSourceFrom+"/FTR_OFER_CRYR")
    lazy val MDM_CRYR = spark.read.parquet(pathSourceFrom+"/MDM_CRYR")

    val F9S_CRYR_LST = FTR_OFER_CRYR.select("OFER_CRYR_CD")
      .distinct()
      .withColumn("CRYR_CD", col("OFER_CRYR_CD"))
      .select("CRYR_CD")
      .join(MDM_CRYR, Seq("CRYR_CD"), "left")
      .select("CRYR_CD","CRYR_NM").withColumn("companyCodes", col("CRYR_CD")).withColumn("companyName",col("CRYR_NM")).drop("CRYR_CD", "CRYR_NM")

    F9S_CRYR_LST.write.mode("overwrite").parquet(pathParquetSave+"/F9S_CRYR_LST")
    F9S_CRYR_LST.write.mode("overwrite").json(pathJsonSave+"/F9S_CRYR_LST")

  }
}
