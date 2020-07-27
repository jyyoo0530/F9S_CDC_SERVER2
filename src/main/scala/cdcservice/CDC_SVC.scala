package cdcservice

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.types.{StructField, StructType}
import query.store.test
import query.{F9S_DSBD_EVNTLOG, F9S_DSBD_RAW, F9S_DSBD_RTELIST, F9S_DSBD_SUM, F9S_DSBD_WKDETAIL, F9S_DSBD_WKLIST, F9S_IDX_LST, F9S_MI_SUM, F9S_MW_BIDASK, F9S_MW_HST, F9S_MW_SUM, F9S_MW_WKDETAIL, F9S_STATS_RAW}

import scala.collection.mutable.ListBuffer

case class CDC_SVC(var spark: SparkSession,
                   var iterIdx: Int,
                   var folderOrigin: String,
                   var folderStats: String,
                   var folderJson: String,
                   var currentWk: String) {
  val list2chk = List(
    "FTR_OFER",
    "FTR_OFER_RTE",
    "FTR_OFER_LINE_ITEM",
    "FTR_OFER_CRYR",
    "FTR_DEAL",
    "FTR_DEAL_LINE_ITEM",
    "FTR_DEAL_CRYR",
    "FTR_DEAL_RTE",
    "FTR_DEAL_RSLT"
  )

  def chk_ID(prop: Properties, url: String): (ListBuffer[Int],ListBuffer[Int]) = {
    ///////////////////////////CDC - DATA LAKE////////////////////////////////////

    var jobTarget = ListBuffer[Int]()
    val idf9s = new ListBuffer[Int]()
    if (iterIdx != 1) {

      // DB Index
      val idTrade = new ListBuffer[Int]()
      for (i <- list2chk.indices) {
        val ID = spark.read.jdbc(url, "ftr." + list2chk(i), prop)
          .select("ID").groupBy().agg(max("ID").as("ID")).collect
          .mkString("").replace("[", "").replace("]", "").toInt
        idTrade += ID
      }

      // DATA LAKE Index

      for (i <- list2chk.indices) {
        val ID = spark.read.parquet(folderOrigin + list2chk(i))
          .select("ID").groupBy().agg(max("ID").as("ID")).collect
          .mkString("").replace("[", "").replace("]", "").toInt
        idf9s += ID
      }
      // job Target ------ choose right runMode according to cold run or not
      jobTarget = (idTrade, idf9s).zipped.map((x, y) => x - y)
    }

    return (jobTarget, idf9s)

  }

  def update_Origin(prop: Properties, url: String, jobTarget: String, targetIdx: Int): DataFrame = {
    // Update Origin Source
    if (iterIdx == 1) // Cold run , update all
        {
        val list2chk2 = list2chk ::: List("MDM_PORT", "MDM_CRYR")
        for (i <- list2chk2.indices) {
          spark.read.jdbc(url, "ftr." + list2chk2(i), prop).write.mode("overwrite").parquet(folderOrigin + list2chk2(i))
        }
        val readResult = spark.emptyDataFrame
        return readResult
      }
      else // CDC run, update only updated columns
      {
        if (targetIdx != 0) {
          val readResult = spark.read.jdbc(url, "ftr." + jobTarget, prop).filter(col("ID") > targetIdx)
//          spark.read.jdbc(url, "ftr." + jobTarget,prop).write.mode("overwrite").parquet(folderOrigin+jobTarget)
          readResult.write.mode("append").parquet(folderOrigin + jobTarget)
          println("Updated " + jobTarget + "with following numbers of rows:") //logger
          println(targetIdx) //logger
          return readResult
        }
        else {
          val readResult = spark.emptyDataFrame
          return readResult
        }
      }
  }

}
