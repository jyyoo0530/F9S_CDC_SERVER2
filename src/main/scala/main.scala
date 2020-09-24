import java.util.{Calendar, GregorianCalendar, Properties}
import java.time.Instant
import java.util.concurrent.TimeUnit

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
import f9s.{appConf, artemisConf, mongoConf, redisConf}
import f9s.core.sparkConf
import org.reactivestreams.Subscription

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import collection.JavaConverters._

object main {
  def main(args: Array[String]): Unit = {

    //// 1) Chk current Week : YYYYWW ////
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
    while (i - i == 0) {
      println("//////////////////////Job " + i.toString + " Started//////////////////////")
      ///// 1) check if there is any updated data in target DB
      i match {
        case 0 => ///// Code Playground --> set appConf.jobIdx => 0 ////

          CDC_SVC(spark, i, currentWk).update_Origin(list2chk(0), 0)
          //////////////////////////////////F9STATS UPDATE START////////////////////////////////
          F9S_DSBD_RAW(spark).dsbd_raw()
          F9S_DSBD_SUM(spark, currentWk).dsbd_sum()

          artemisConf
//          val (jobTarget, idf9s) = CDC_SVC(spark, i, currentWk).chk_ID()
//          if (jobTarget.sum != 0) ///// 2) ignore if there is no updates
//            {
//              val FTR_OFER = CDC_SVC(spark, i, currentWk).update_Origin(list2chk(0), idf9s(0))
//              val FTR_OFER_RTE = CDC_SVC(spark, i, currentWk).update_Origin(list2chk(1), idf9s(1))
//              val FTR_OFER_LINE_ITEM = CDC_SVC(spark, i, currentWk).update_Origin(list2chk(2), idf9s(2))
//              val FTR_OFER_CRYR = CDC_SVC(spark, i, currentWk).update_Origin(list2chk(3), idf9s(3))
//              val FTR_DEAL = CDC_SVC(spark, i, currentWk).update_Origin(list2chk(4), idf9s(4))
//              val FTR_DEAL_LINE_ITEM = CDC_SVC(spark, i, currentWk).update_Origin(list2chk(5), idf9s(5))
//              val FTR_DEAL_CRYR = CDC_SVC(spark, i, currentWk).update_Origin(list2chk(6), idf9s(6))
//              val FTR_DEAL_RTE = CDC_SVC(spark, i, currentWk).update_Origin(list2chk(7), idf9s(7))
//              val FTR_DEAL_RSLT = CDC_SVC(spark, i, currentWk).update_Origin(list2chk(8), idf9s(8))
//
//              //// 4) appending F9STATS start//
//              val offerNumbers: List[String] =
//                FTR_OFER.select("OFER_NR")
//                  .distinct.rdd.map(r => r(0).asInstanceOf[String].split("\\|").map(_.toString).distinct).collect().flatten.toList
//              print(offerNumbers)
//              val userId: List[String] =
//                FTR_OFER.select("EMP_NR")
//                  .distinct.rdd.map(r => r(0).asInstanceOf[String].split("\\|").map(_.toString).distinct).collect().flatten.toList
//              print(userId)
//
//              /////////// DSBD Update Block Start ///////
//              F9S_DSBD_RAW(spark).append_dsbd_raw(FTR_OFER, FTR_OFER_CRYR, FTR_OFER_RTE, FTR_OFER_LINE_ITEM)
//
//              def deleteMongoSeats(collectionName: String, deleteFrom: String, deleteQuery: List[String]): Unit = {
//                val collection: MongoCollection[Document] = mongoConf.database.getCollection(collectionName)
//                val observable: Observable[DeleteResult] = collection.deleteMany(Filters.in(deleteFrom, deleteQuery: _*))
//                val observer: Observer[DeleteResult] = new Observer[DeleteResult] {
//                  override def onNext(result: DeleteResult): Unit = println("Delete")
//
//                  override def onError(e: Throwable): Unit = println("Failed")
//
//                  override def onComplete(): Unit = println("Completed")
//                }
//                observable.subscribe(observer)
//                System.out.println(Await.result(observable.toFuture, Duration(10, TimeUnit.SECONDS)))
//              }
//
//              deleteMongoSeats("F9S_DSBD_SUM", "userId", userId)
//              deleteMongoSeats("F9S_DSBD_WKDETAIL", "offerNumber", offerNumbers)
//              deleteMongoSeats("F9S_DSBD_EVNTLOG", "offerNumber", offerNumbers)
//              deleteMongoSeats("F9S_DSBD_RTELIST", "userId", userId)
//              deleteMongoSeats("F9S_DSBD_WKLIST", "userId", userId)
//
//              F9S_DSBD_SUM(spark, currentWk).append_dsbd_sum(userId)
//              F9S_DSBD_WKDETAIL(spark).append_dsbd_wkdetail(offerNumbers)
//              F9S_DSBD_EVNTLOG(spark, currentWk).append_dsbd_evntlog(offerNumbers)
//              F9S_DSBD_RTELIST(spark).append_dsbd_rtelist(userId)
//              F9S_DSBD_WKLIST(spark).append_dsbd_wklist(userId)
//              /////////// DSBD Update Block End ////////////
//
//              println("/////////////////////Job " + i.toString + " Finished//////////////////////")
//            }
        case 1 => ///// Cold Run -> get all data and update all
          CDC_SVC(spark, i, currentWk).update_Origin(list2chk(0), 0)
          //////////////////////////////////F9STATS UPDATE START////////////////////////////////
          F9S_DSBD_RAW(spark).dsbd_raw()
          F9S_DSBD_SUM(spark, currentWk).dsbd_sum()
          F9S_DSBD_WKDETAIL(spark).dsbd_wkdetail()
          F9S_DSBD_EVNTLOG(spark, currentWk).dsbd_evntlog()
          F9S_DSBD_RTELIST(spark).dsbd_rtelist()
          F9S_DSBD_WKLIST(spark).dsbd_wklist()

          F9S_STATS_RAW(spark).stats_raw()
          F9S_MW_SUM(spark, currentWk).mw_sum()
          F9S_MW_HST(spark).mw_hst()
          F9S_MW_WKDETAIL(spark).mw_wkdetail()
          F9S_MW_BIDASK(spark).mw_bidask()
          F9S_MI_SUM(spark).mi_sum()
          F9S_IDX_LST(spark).idx_lst()
          println("/////////////////////Job " + i.toString + " Finished//////////////////////")
          i += 1
        case _ => ///// CDC function
          val (jobTarget, idf9s) = CDC_SVC(spark, i, currentWk).chk_ID()
          if (jobTarget.sum != 0) ///// 2) ignore if there is no updates
            {
              val FTR_OFER = CDC_SVC(spark, i, currentWk).update_Origin(list2chk(0), idf9s(0))
              val FTR_OFER_RTE = CDC_SVC(spark, i, currentWk).update_Origin(list2chk(1), idf9s(1))
              val FTR_OFER_LINE_ITEM = CDC_SVC(spark, i, currentWk).update_Origin(list2chk(2), idf9s(2))
              val FTR_OFER_CRYR = CDC_SVC(spark, i, currentWk).update_Origin(list2chk(3), idf9s(3))
              val FTR_DEAL = CDC_SVC(spark, i, currentWk).update_Origin(list2chk(4), idf9s(4))
              val FTR_DEAL_LINE_ITEM = CDC_SVC(spark, i, currentWk).update_Origin(list2chk(5), idf9s(5))
              val FTR_DEAL_CRYR = CDC_SVC(spark, i, currentWk).update_Origin(list2chk(6), idf9s(6))
              val FTR_DEAL_RTE = CDC_SVC(spark, i, currentWk).update_Origin(list2chk(7), idf9s(7))
              val FTR_DEAL_RSLT = CDC_SVC(spark, i, currentWk).update_Origin(list2chk(8), idf9s(8))

              //// 4) appending F9STATS start//
              val offerNumbers: List[String] =
                FTR_OFER.select("OFER_NR")
                  .distinct.rdd.map(r => r(0).asInstanceOf[String].split("\\|").map(_.toString).distinct).collect().flatten.toList
              print(offerNumbers)
              val userId: List[String] =
                FTR_OFER.select("EMP_NR")
                  .distinct.rdd.map(r => r(0).asInstanceOf[String].split("\\|").map(_.toString).distinct).collect().flatten.toList
              print(userId)

              /////////// DSBD Update Block Start ///////
              F9S_DSBD_RAW(spark).append_dsbd_raw(FTR_OFER, FTR_OFER_CRYR, FTR_OFER_RTE, FTR_OFER_LINE_ITEM)

              def deleteMongoSeats(collectionName: String, deleteFrom: String, deleteQuery: List[String]): Unit = {
                val collection: MongoCollection[Document] = mongoConf.database.getCollection(collectionName)
                val observable: Observable[DeleteResult] = collection.deleteMany(Filters.in(deleteFrom, deleteQuery: _*))
                val observer: Observer[DeleteResult] = new Observer[DeleteResult] {
                  override def onNext(result: DeleteResult): Unit = println("Delete")

                  override def onError(e: Throwable): Unit = println("Failed")

                  override def onComplete(): Unit = println("Completed")
                }
                observable.subscribe(observer)
                System.out.println(Await.result(observable.toFuture, Duration(10, TimeUnit.SECONDS)))
              }

              deleteMongoSeats("F9S_DSBD_SUM", "userId", userId)
              deleteMongoSeats("F9S_DSBD_WKDETAIL", "offerNumber", offerNumbers)
              deleteMongoSeats("F9S_DSBD_EVNTLOG", "offerNumber", offerNumbers)
              deleteMongoSeats("F9S_DSBD_RTELIST", "userId", userId)
              deleteMongoSeats("F9S_DSBD_WKLIST", "userId", userId)

              F9S_DSBD_SUM(spark, currentWk).append_dsbd_sum(userId)
              F9S_DSBD_WKDETAIL(spark).append_dsbd_wkdetail(offerNumbers)
              F9S_DSBD_EVNTLOG(spark, currentWk).append_dsbd_evntlog(offerNumbers)
              F9S_DSBD_RTELIST(spark).append_dsbd_rtelist(userId)
              F9S_DSBD_WKLIST(spark).append_dsbd_wklist(userId)
              /////////// DSBD Update Block End ////////////


              println("/////////////////////Job " + i.toString + " Finished//////////////////////")
              i += 1
            } else {
            i += 1
          }
      }
    }

  }
}
