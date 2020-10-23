package f9s.core.cdc

import java.util.Properties

import f9s._
import f9s.core.query.F9S_STATS_RAW
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import scala.collection.mutable.ListBuffer

case class CDC_SVC(var spark: SparkSession,
                   var iterIdx: Int,
                   var currentWk: String) {

  val filePath: String = appConf().dataLake match {
    case "file" => appConf().folderOrigin
    case "hadoop" => hadoopConf.hadoopPath
  }

  def getUpdatedData(idList: List[(String, Int, Int)], ftrData: List[(String, DataFrame)]): List[(String, DataFrame)] = {
    val tableNm = idList.map(_._1)
    val dbId = idList.map(_._2)
    val f9sId = idList.map(_._3)
    var ftrUpdatedDataSnapshot = List[(String, DataFrame)]()
    for (i <- ftrData.indices) {
      if (dbId(i) > f9sId(i)) {
        val cachedData = ftrData.find(_._1 == tableNm(i)).get._2.cache.toDF
        ftrUpdatedDataSnapshot = ftrUpdatedDataSnapshot ::: List((tableNm(i), cachedData))
      }
    }
    ftrUpdatedDataSnapshot
  }

  def updateF9sId(idList: List[(String, Int, Int)], ftrUpdatedDataSnapshot: List[(String, DataFrame)]): List[Int] = {
    val tableNm = idList.map(_._1)
    val dbId = idList.map(_._2)
    var newF9sId = List[(String, Int)]()
    for (i <- idList.indices) {
      try {
        val newIdx = ftrUpdatedDataSnapshot.find(_._1 == tableNm(i)).get._2
          .select("ID").groupBy().agg(max("ID").as("ID")).collect
          .mkString("").replace("[", "").replace("]", "").toInt
        newF9sId = newF9sId ::: List((tableNm(i), newIdx))
        println(newIdx)
        System.out.println(":     '" + tableNm(i) + "'  '" + dbId(i).toString + "'     " + "UPDATED")
      }
      catch {
        case e: Exception =>
          newF9sId = newF9sId ::: List((tableNm(i), dbId(i)))
          System.out.println(":     '" + tableNm(i) + "'  '" + dbId(i).toString + "'     " + "Not Existing in ftrUpdatedDataSnapshot")
      }
    }
    if (newF9sId.nonEmpty) {
      println("........f9sId updating..........")
      println(": Updated")
      newF9sId.map(_._2)
    }
    else {
      println("!!No ID to be Updated!!")
      newF9sId.map(_._2)
    }
  }

  def getFtrUpdatedDataOnlySnapshot(ftrUpdatedDataSnapshot: List[(String, DataFrame)], idList: List[(String, Int, Int)]): List[(String, DataFrame)] = {
    val tableNm = idList.map(_._1)
    val dbId = idList.map(_._2)
    val f9sId = idList.map(_._3)
    var ftrUpdatedDataOnlySnapshot = List[(String, DataFrame)]()
    for (i <- ftrUpdatedDataSnapshot.indices) {
      try {
        val updatedData = ftrUpdatedDataSnapshot.find(_._1 == tableNm(i)).get._2.unpersist.filter(col("ID") > f9sId(i)).cache.toDF
        println(updatedData.count) ///////////디버깅, 추후 삭제
        ftrUpdatedDataOnlySnapshot = ftrUpdatedDataOnlySnapshot ::: List((tableNm(i), updatedData))
      } catch {
        case e: Exception =>
          println("error caused!!")
          ftrUpdatedDataOnlySnapshot.updated(i, List(tableNm(i), spark.emptyDataFrame))
      }
    }
    ftrUpdatedDataOnlySnapshot
  }

  def makeMarketWatchTopicList(f9sStatsRawUpdatedDataOnlySnapshot: DataFrame, targetTable: String): DataFrame = {
    var f9sUpdatedTopicList = spark.emptyDataFrame
    targetTable match {
      case "f9sUpdatedTopicsMarketWatch" => {
        try {
          f9sUpdatedTopicList = f9sStatsRawUpdatedDataOnlySnapshot.select(col("polCode"), col("podCode")).distinct
        }
        catch {
          case e: Exception =>
            f9sUpdatedTopicList = spark.emptyDataFrame
            println("There is no data to be updated")
        }

      }
      case "f9sUpdatedTopicsProductWeekDetail" => {
        try {
          f9sUpdatedTopicList = f9sStatsRawUpdatedDataOnlySnapshot
            .select(col("polCode"), col("podCode"), col("baseYearWeek")).distinct
        } catch {
          case e: Exception =>
            f9sUpdatedTopicList = spark.emptyDataFrame
            println("There is no data to be updated")
        }
      }
      case "f9sUpdatedTopicsProductDealHistory" => {
        try {
          f9sUpdatedTopicList = f9sStatsRawUpdatedDataOnlySnapshot.select(col("polCode"), col("podCode"), col("baseYearWeek")).distinct
        } catch {
          case e: Exception =>
            f9sUpdatedTopicList = spark.emptyDataFrame
            println("There is no data to be updated")
        }

      }
      case "f9sUpdatedTopicsMarketIndex" => {
        // 다른 프로세스
      }
    }
    f9sUpdatedTopicList
  }
}

