package f9s.core.cdc

import java.util.Properties

import f9s._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.types.{StructField, StructType}
import scala.util.{Try, Success, Failure}

import scala.collection.mutable.ListBuffer

case class CDC_SVC(var spark: SparkSession,
                   var iterIdx: Int,
                   var currentWk: String) {

  val filePath: String = appConf().dataLake match {
    case "file" => appConf().folderOrigin
    case "hadoop" => hadoopConf.hadoopPath
  }
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

  def chk_ID(): (ListBuffer[Int], ListBuffer[Int]) = {
    ///////////////////////////CDC - DATA LAKE////////////////////////////////////

    var jobTarget = ListBuffer[Int]()
    val idf9s = new ListBuffer[Int]()
    if (iterIdx != 1) {

      // DB Index
      val idTrade = new ListBuffer[Int]()
      for (i <- list2chk.indices) {
        try {
          val ID = spark.read.jdbc(jdbcConf.url, "ftr." + list2chk(i), jdbcConf.prop)
            .select("ID").groupBy().agg(max("ID").as("ID")).collect
            .mkString("").replace("[", "").replace("]", "").toInt
          idTrade += ID
        } catch {
          case e: Exception => println("!!!!!!!!Unknown Error Cases!!!!!!!!")
        }
      }

      // DATA LAKE Index
      for (i <- list2chk.indices) {
        try {
          val ID = spark.read.parquet(filePath + list2chk(i))
            .select("ID").groupBy().agg(max("ID").as("ID")).collect
            .mkString("").replace("[", "").replace("]", "").toInt
          idf9s += ID
        }
        catch {
          case e: Exception => println("!!!!!!!!Unknown Error Cases!!!!!!!!")
        }
      }
      // job Target ------ choose right runMode according to cold run or not
      jobTarget = (idTrade, idf9s).zipped.map((x, y) => x - y)
    }

    return (jobTarget, idf9s)

  }

  def update_Origin(jobTarget: String, targetIdx: Int): DataFrame = {
    // Update Origin Source
    if (iterIdx == 1) // Cold run , main all
      {
        val list2chk2 = list2chk ::: List("MDM_PORT", "MDM_CRYR")
        for (i <- list2chk2.indices) {
          spark.read.jdbc(jdbcConf.url, "ftr." + list2chk2(i), jdbcConf.prop)
            .write.mode("overwrite")
            .parquet(filePath + list2chk2(i))
        }
        val readResult = spark.emptyDataFrame
        return readResult
      }
      else // CDC run, main only updated columns
      {
        if (targetIdx != 0) {
          val readResult = spark.read.jdbc(jdbcConf.url, "ftr." + jobTarget, jdbcConf.prop).filter(col("ID") > targetIdx)
          readResult.write.mode("append").parquet(filePath + jobTarget)
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

