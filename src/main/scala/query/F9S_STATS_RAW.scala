package query

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class F9S_STATS_RAW(var spark: SparkSession, var pathSourceFrom: String,
                         var pathParquetSave: String, var pathJsonSave: String) {
  def stats_raw() {
    val F9S_STATS_RAW = spark.read.parquet(pathSourceFrom + "/FTR_DEAL_RTE")
      .filter(col("TRDE_LOC_TP_CD") === "02" || col("TRDE_LOC_TP_CD") === "03")
      .withColumn("a1", col("OFER_NR"))
      .withColumn("a2", col("OFER_CHNG_SEQ"))
      .withColumn("DEAL_DATE", col("DEAL_NR").substr(lit(2), lit(20)))
      .drop("DEL_YN", "ID", "CRE_USR_ID", "UPD_USR_ID", "DEAL_ID", "DEAL_CHNG_SEQ")

      .crossJoin(
        spark.read.parquet(pathSourceFrom + "/FTR_DEAL_RSLT")
          .withColumn("b1", col("OFER_NR"))
          .withColumn("b2", col("OFER_CHNG_SEQ"))
          .select("DEAL_AMT", "DEAL_CHNG_SEQ", "DEAL_PRCE", "DEAL_QTY", "DEAL_SKIP_YN", "DEAL_SUCC_YN", "DEAL_YW", "REG_SEQ", "b1", "b2")
      )
      .withColumn("a", concat(col("a2"), col("a1")))
      .withColumn("b", concat(col("b2"), col("b1")))
      .drop("b1", "b2")
      .filter(col("a") === col("b"))
      .drop("a", "b")

    F9S_STATS_RAW.write.mode("append").parquet(pathParquetSave + "/F9S_STATS_RAW")
  }
}
