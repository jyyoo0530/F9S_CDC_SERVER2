import java.util.Properties

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import scala.collection.mutable.ListBuffer
import query._
import java.io.File
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.serializer.KryoSerializer
import com.mongodb.spark._
import org.bson.Document

object main {
  def main(args: Array[String]): Unit = {

    //// choose your mode ////
    val currentWk = "202028"
    //    val runMode = "CDC RUN"
    val runMode = "coldRun"
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    ////////////////////////////////SPARK SESSION///////////////////////////////////
    val master = "local[*]"
    //    val master = "spark://192.168.0.6:7077"
    val appName = "MyApp"
    val conf: SparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      //      .set("spark.driver.allowMultipleContexts", "false")
      .set("spark.ui.enabled", "true")
      .set("spark.ui.port", "5555")
      //      .set("spark.driver.cores", "2")
      //      .set("spark.driver.memory", "12g")
      .set("spark.executor.memoryOverhead", "1g")
      ////      .set("spark.cores.max", "4")
      //      .set("spark.executor.memory", "10g")
      ////      .set("spark.speculation", "true")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //      .set("spark.executor.instances", "2")
    //      .set("spark.sql.shuffle.partitions", "300")
    //      .set("spark.sql.files.maxPartitionBytes", "13421772")
      .set("spark.mongodb.input.uri", "mongodb://data.freight9.com:27017")
      .set("spark.mongodb.output.uri", "mongodb://data.freight9.com:27017")
      .set("spark.mongodb.input.database", "f9s")
      .set("spark.mongodb.output.database", "f9s")



    val spark = SparkSession.builder()
      .config(conf)
      .appName("CDC FUNCTION")
      .getOrCreate()




    /// path setting //
    val rootPath = new java.io.File(".").getCanonicalPath + "/src/main/resources/"

    val folderOrigin = "file:/" + rootPath + "OriginSource/"
    val folderStats = "file:/" + rootPath + "stats/"
    val folderJSON = "file:/" + rootPath + "JSON/"

    val folderOriginTest = "file:/" + rootPath + "test/OriginSource/"
    val folderStatsTest = "file:/" + rootPath + "test/stats/"
    val folderJSONTest = "file:/" + rootPath + "test/JSON/"

    val a = folderOrigin
    val b = folderStatsTest
    val c = folderJSONTest


//    ///////////////////////////CDC - DATA LAKE////////////////////////////////////
//        val list2chk = List("FTR_OFER", "FTR_OFER_RTE", "FTR_OFER_LINE_ITEM", "FTR_OFER_CRYR", "FTR_DEAL", "FTR_DEAL_LINE_ITEM", "FTR_DEAL_CRYR", "FTR_DEAL_RTE", "FTR_DEAL_RSLT")
//
//        //JDBC SESSION//
//        val prop = new Properties()
//        prop.put("user", "ftradm")
//        prop.put("password", "12345678")
//        val url = "jdbc:mysql://opus365-dev01.cbqbqnguxslu.ap-northeast-2.rds.amazonaws.com:3306"
//
//        var jobTarget = ListBuffer[Int]()
//        if (runMode != "coldRun") {
//          // DB Index
//          val idTrade = new ListBuffer[Int]()
//          for (i <- list2chk.indices) {
//            val ID = spark.read.jdbc(url, "ftr." + list2chk(i), prop)
//              .select("ID").groupBy().agg(max("ID").as("ID")).collect
//              .mkString("").replace("[", "").replace("]", "").toInt
//            println(ID) //logger
//            idTrade += ID
//          }
//
//          // DATA LAKE Index
//          val idf9s = new ListBuffer[Int]()
//          for (i <- list2chk.indices) {
//            val ID = spark.read.parquet(a + list2chk(i))
//              .select("ID").groupBy().agg(max("ID").as("ID")).collect
//              .mkString("").replace("[", "").replace("]", "").toInt
//            println(ID) //LOGGER
//            idf9s += ID
//          }
//          // job Target ------ choose right runMode according to cold run or not
//          jobTarget = (idTrade, idf9s).zipped.map((x, y) => x - y)
//        }
//
//        // Update Origin Source
//        if (runMode == "coldRun") {
//          val list2chk2 = List("MDM_CRYR", "MDM_PORT", "FTR_OFER", "FTR_OFER_RTE", "FTR_OFER_LINE_ITEM", "FTR_OFER_CRYR", "FTR_DEAL", "FTR_DEAL_LINE_ITEM", "FTR_DEAL_CRYR", "FTR_DEAL_RTE", "FTR_DEAL_RSLT")
//          for (i <- list2chk2.indices) {
//            spark.read.jdbc(url, "ftr." + list2chk2(i), prop).write.mode("overwrite").parquet(a + list2chk2(i))
//          }
//        }
//        else {
//          for (i <- list2chk.indices) {
//            if (jobTarget(i) != 0) {
//              var updater = spark.read.jdbc(url, "ftr." + list2chk(i), prop).filter(col("ID") > jobTarget(i))
//              updater.write.mode("append").parquet(a + list2chk(i))
//            } else {
//              println("JOB SKIPPED " + list2chk(i))
//            }
//          }
//        }

    //////////////////////////////////F9STATS UPDATE START////////////////////////////////
    //        F9S_CRYR_LST(spark, a, b, c).cryr_lst()
//    F9S_DSBD_RTELIST(spark, a, b, c).dsbd_rtelist()
//    F9S_DSBD_WKLIST(spark, a, b, c).dsbd_wklist()
//    F9S_DSBD_SUM(spark, a, b, c, currentWk).dsbd_sum()
//    F9S_DSBD_WKDETAIL(spark, a, b, c).dsbd_wkdetail()
    F9S_DSBD_EVNTLOG(spark, a, b, c, currentWk).dsbd_evntlog()
//    F9S_STATS_RAW(spark, a, b, c).stats_raw()
//    F9S_MW_SUM(spark, a, b, c, currentWk).mw_sum()
//    F9S_MW_HST(spark, a, b, c).mw_hst()
//    F9S_MW_WKDETAIL(spark, a, b, c).mw_wkdetail()
//    F9S_MW_BIDASK(spark, a, b, c).mw_bidask()
//    F9S_MI_SUM(spark, a, b, c).mi_sum()
//    F9S_IDX_LST(spark, b, c).idx_lst()
//

  }
}
