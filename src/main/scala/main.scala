package F9S_CORE

import java.util.{Calendar, GregorianCalendar, Properties}

import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.expressions.Window
import org.mongodb.scala._
import org.mongodb.scala.model.Filters
import com.mongodb.client.result.DeleteResult

import scala.collection.JavaConverters._
import org.apache.spark.sql._
import f9s.core.query._
import f9s.core.cdc._
import f9s.{appConf, mongoConf, redisConf}
import f9s.core.sparkConf


object main {
  def main(args: Array[String]): Unit = {

    //// 1) Chk current Week ////
    val calendar = new GregorianCalendar()
    val currentWk = if (calendar.get(Calendar.WEEK_OF_YEAR).toString.length == 1) {
      calendar.get(Calendar.YEAR).toString + "0" + calendar.get(Calendar.WEEK_OF_YEAR).toString
    } else {
      calendar.get(Calendar.YEAR).toString + calendar.get(Calendar.WEEK_OF_YEAR).toString
    }

    //// 2) App settings ////
    //Logger
    if (appConf().sparkLogger == "OFF") {
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
    } else if (appConf().sparkLogger == "FATAL") {
      Logger.getLogger("org").setLevel(Level.FATAL)
      Logger.getLogger("akka").setLevel(Level.FATAL)
    } else if (appConf().sparkLogger == "ALL") {
      Logger.getLogger("org").setLevel(Level.ALL)
      Logger.getLogger("akka").setLevel(Level.ALL)
    }
    //Filepath
    val a = if (appConf().pathMode == "TEST") {
      appConf().folderOriginTest
    } else {
      appConf().folderOrigin
    }
    val b = if (appConf().pathMode == "TEST") {
      appConf().folderStatsTest
    } else {
      appConf().folderStats
    }
    val c = if (appConf().pathMode == "TEST") {
      appConf().folderJSONTest
    } else {
      appConf().folderJSON
    }
    //Table List to check
    val list2chk = appConf().list2chk

    ////////////////////////////////SPARK SESSION///////////////////////////////////

    val spark = SparkSession.builder()
      .config(sparkConf().conf)
      .appName(sparkConf().appName)
      .getOrCreate()

//    redisConf.connection.sync().set("Jeremy", "Super Genius!!")
//    println(redisConf.connection.sync().get("Jeremy"))
//    redisConf.connection.close()




    //////// MAIN SERVICE STARTED //////
    var i = appConf().jobIdx
    while (i / i == 1) {
      println("//////////////////////Job " + i.toString + " Started//////////////////////")

      ///// 1) check if there is any updated data in target DB
      if (i != 1) {
        val (jobTarget, idf9s) = CDC_SVC(spark, i, a, b, c, currentWk).chk_ID
        if (jobTarget.sum == 0)  ///// 2) ignore if there is no updates
          {
            i += 1
          }
          else ////3) save updated data from DB at the thread
          {
            val FTR_OFER = CDC_SVC(spark, i, a, b, c, currentWk).update_Origin(list2chk(0), idf9s(0))
            val FTR_OFER_RTE = CDC_SVC(spark, i, a, b, c, currentWk).update_Origin(list2chk(1), idf9s(1))
            val FTR_OFER_LINE_ITEM = CDC_SVC(spark, i, a, b, c, currentWk).update_Origin(list2chk(2), idf9s(2))
            val FTR_OFER_CRYR = CDC_SVC(spark, i, a, b, c, currentWk).update_Origin(list2chk(3), idf9s(3))
            val FTR_DEAL = CDC_SVC(spark, i, a, b, c, currentWk).update_Origin(list2chk(4), idf9s(4))
            val FTR_DEAL_LINE_ITEM = CDC_SVC(spark, i, a, b, c, currentWk).update_Origin(list2chk(5), idf9s(5))
            val FTR_DEAL_CRYR = CDC_SVC(spark, i, a, b, c, currentWk).update_Origin(list2chk(6), idf9s(6))
            val FTR_DEAL_RTE = CDC_SVC(spark, i, a, b, c, currentWk).update_Origin(list2chk(7), idf9s(7))
            val FTR_DEAL_RSLT = CDC_SVC(spark, i, a, b, c, currentWk).update_Origin(list2chk(8), idf9s(8))

            //// 4) appending F9STATS start//
            val offerNumbers = FTR_OFER.select("OFER_NR").distinct.rdd.map(r => r(0).asInstanceOf[String].split("\\|").map(_.toString).distinct).collect().flatten.toSeq
            print(offerNumbers)
            val userId = FTR_OFER.select("EMP_NR").distinct.rdd.map(r => r(0).asInstanceOf[String].split("\\|").map(_.toString).distinct).collect().flatten.toSeq
            print(userId)

            F9S_DSBD_RAW(spark, a, b, c).append_dsbd_raw(FTR_OFER, FTR_OFER_CRYR, FTR_OFER_RTE, FTR_OFER_LINE_ITEM)

            val collection1: MongoCollection[Document] = mongoConf.database.getCollection("F9S_DSBD_SUM")
            val test = collection1.deleteMany(Filters.in("userId", userId))
            val collection2: MongoCollection[Document] = mongoConf.database.getCollection("F9S_DSBD_WKDETAIL")
            collection2.deleteMany(Filters.in("offerNumber", offerNumbers))
            val collection3: MongoCollection[Document] = mongoConf.database.getCollection("F9S_DSBD_EVNTLOG")
            collection3.deleteMany(Filters.in("offerNumber", offerNumbers))

            F9S_DSBD_SUM(spark, a, b, c, currentWk).append_dsbd_sum(userId)
            F9S_DSBD_WKDETAIL(spark, a, b, c).append_dsbd_wkdetail(offerNumbers)
            F9S_DSBD_EVNTLOG(spark, a, b, c, currentWk).append_dsbd_evntlog(offerNumbers)

            val collection4: MongoCollection[Document] = mongoConf.database.getCollection("F9S_DSBD_RTELIST")
            collection4.deleteMany(Filters.in("userId", userId))
            val collection5: MongoCollection[Document] = mongoConf.database.getCollection("F9S_DSBD_WKLIST")
            collection5.deleteMany(Filters.in("userId", userId))

            F9S_DSBD_RTELIST(spark, a, b, c).append_dsbd_rtelist(userId)
            F9S_DSBD_WKLIST(spark, a, b, c) append_dsbd_wklist (userId)


            println("/////////////////////Job " + i.toString + " Finished//////////////////////")
            i += 1
          }
      }
      else //// get and main all database in case of service's COLD RUN
      {
        CDC_SVC(spark, i, a, b, c, currentWk).update_Origin(list2chk(0), 0)
        //////////////////////////////////F9STATS UPDATE START////////////////////////////////
        F9S_DSBD_RAW(spark, a, b, c).dsbd_raw()
        F9S_DSBD_SUM(spark, a, b, c, currentWk).dsbd_sum()
        F9S_DSBD_WKDETAIL(spark, a, b, c).dsbd_wkdetail()
        F9S_DSBD_EVNTLOG(spark, a, b, c, currentWk).dsbd_evntlog()
        F9S_DSBD_RTELIST(spark, a, b, c).dsbd_rtelist()
        F9S_DSBD_WKLIST(spark, a, b, c).dsbd_wklist()

        F9S_STATS_RAW(spark, a, b, c).stats_raw()
        F9S_MW_SUM(spark, a, b, c, currentWk).mw_sum()
        F9S_MW_HST(spark, a, b, c).mw_hst()
        F9S_MW_WKDETAIL(spark, a, b, c).mw_wkdetail()
        F9S_MW_BIDASK(spark, a, b, c).mw_bidask()
        F9S_MI_SUM(spark, a, b, c).mi_sum()
        F9S_IDX_LST(spark, b, c).idx_lst()
        println("/////////////////////Job " + i.toString + " Finished//////////////////////")
        i += 1
      }
    }

  }
}
