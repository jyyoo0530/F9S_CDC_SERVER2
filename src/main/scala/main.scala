import java.util.{Calendar, GregorianCalendar, Properties}
import java.time.Instant
import java.util.concurrent.TimeUnit

import com.google.gson.{Gson, JsonElement, JsonObject, JsonParser}

import scala.util.Random
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
import com.mongodb.spark.MongoSpark
import f9s.core.artemisbroker.ArtemisProducer

import scala.collection.JavaConverters._
import org.apache.spark.sql._
import f9s.core.query.{_garbage, _}
import f9s.core.cdc._
import f9s.core.model.ftr.{FTR_OFER, FTR_Schema}
import f9s.core.query._garbage.{F9S_DSBD_EVNTLOG, F9S_DSBD_RAW, F9S_DSBD_RTELIST, F9S_DSBD_SUM, F9S_DSBD_WKDETAIL, F9S_DSBD_WKLIST}
import f9s.{appConf, artemisConf, hadoopConf, jdbcConf, mongoConf, redisConf}
import f9s.core.sparkConf
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import javax.jms.{Connection, Message, MessageConsumer, MessageProducer, Session, TextMessage, Topic}
import javax.naming.InitialContext
import org.apache.activemq.artemis.jms.client.{ActiveMQConnection, ActiveMQConnectionFactory, ActiveMQMessageProducer, ActiveMQSession, ActiveMQTopic}
import org.apache.commons.beanutils.{BeanUtils, BeanUtilsBean, PropertyUtilsBean}
import org.apache.commons.net.ntp.TimeStamp
import org.apache.spark.rdd.RDD
import org.reactivestreams.Subscription
import org.sparkproject.jetty.util.SocketAddressResolver.Async
import shaded.parquet.org.codehaus.jackson.map.util.BeanUtil

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import collection.JavaConverters._
import scala.beans.BeanProperty
import scala.util.parsing.json.JSONObject

object main {
  def main(args: Array[String]): Unit = {

    val filePath = appConf().dataLake match {
      case "file" => appConf().folderOrigin
      case "hadoop" => hadoopConf.hadoopPath
    }

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

    ////// Declaration of basic variables //////
    var idList = List[(String, Int, Int)]()
    var ftrUpdatedDataSnapshot = List[(String, DataFrame)]()
    var ftrUpdatedDataOnlySnapshot = List[(String, DataFrame)]()
    var ftrData = List[(String, DataFrame)]()
    var ftrDataSnapshot = List[(String, DataFrame)]()
    var f9sStatsRaw = spark.emptyDataFrame
    var f9sId = List[Int]()
    var dbId = List[Int]()
    var mdmCryr = spark.emptyDataFrame

    ///// Declaration of market watch data /////
    var f9sMwSum = spark.emptyDataFrame
    var f9sMwHst = spark.emptyDataFrame
    var f9sMwWkDetail = spark.emptyDataFrame

    def ftrSourceCaching(a: Int): List[(String, DataFrame)] = {
      val srcData = CDC_SVC_COLD(spark, i, currentWk).getData
      val tableNm = srcData.map(_._1)
      a match {
        case 1 => // return Cached Data
          for (i <- srcData.indices) { // return Cached Data
            srcData.find(_._1 == tableNm(i)).get._2.createOrReplaceTempView(tableNm(i))
            srcData.updated(i, (tableNm(i), spark.read.table(tableNm(i)).cache))
          }
          srcData

        case 2 => // return Pointer Data
          srcData

      }
    }

    def ftrGetId_fromSource(a: List[(String, DataFrame)]): List[Int] = {
      var result = List[Int]()
      val tableNm = a.map(_._1)
      for (i <- a.indices) {
        result = result ::: List(a.find(_._1 == tableNm(i)).get._2
          .select("ID").groupBy().agg(max("ID").as("ID")).collect
          .mkString("").replace("[", "").replace("]", "").toInt)
      }
      result
    }

    def ftrConcatIdList(a: List[Int], b: List[Int], c: List[String]): List[(String, Int, Int)] = {
      var idList = List[(String, Int, Int)]()
      for (i <- c.indices) {
        idList = idList ::: List((c(i), a(i), b(i)))
      }
      idList
    }

    def f9sWrite2DB(saveData: DataFrame, table: String, mode: String): Unit = {
      mode match {
        case "overwrite" =>
          MongoSpark.save(saveData.write.option("uri", mongoConf.sparkMongoUri)
            .option("database", "f9s")
            .option("collection", table)
            .mode("overwrite"))

        case "append" =>
          MongoSpark.save(saveData.write.option("uri", mongoConf.sparkMongoUri)
            .option("database", "f9s")
            .option("collection", table)
            .mode("append"))

      }

    }

    def topicMapper(jsonSet: String, category: String): String = {  ////////////////OBSOLETE REFERENCE ONLY//////////////
      var result = "" // return
      val jobject: JsonObject = new JsonParser().parse(jsonSet).getAsJsonObject
      val got1 = jobject.getAsJsonObject().getAsJsonPrimitive("polCode").toString.replaceAll("[^a-zA-Z0-9_-]", "")
      val got2 = jobject.getAsJsonObject().getAsJsonPrimitive("podCode").toString.replaceAll("[^a-zA-Z0-9_-]", "")
      result = "/topic/" + "01_01_01_" + got1 + "_" + got2 + "_01"
      result

    }

    def deleteMongoSeats(collectionName: String, targetTopics: DataFrame): Unit = {
      collectionName match {
        case "F9S_MW_SUM" =>
          val collection: MongoCollection[Document] = mongoConf.database.getCollection(collectionName)
          val polCodes = targetTopics.select("polCode")
            .rdd.map(r => r(0).asInstanceOf[String].split("\\|").map(_.toString)).collect().flatten.toList
          val podCodes = targetTopics.select("podCode")
            .rdd.map(r => r(0).asInstanceOf[String].split("\\|").map(_.toString)).collect().flatten.toList
          for (i <- polCodes.indices) {
            val observable: Observable[DeleteResult] = collection
              .deleteMany(Filters.and(Filters.in("polCode", polCodes(i)), Filters.in("podCode", podCodes(i))))
            val observer: Observer[DeleteResult] = new Observer[DeleteResult] {
              override def onNext(result: DeleteResult): Unit = println("Delete")

              override def onError(e: Throwable): Unit = println("Failed")

              override def onComplete(): Unit = println("Completed")
            }
            observable.subscribe(observer)
            System.out.println(Await.result(observable.toFuture, Duration(10, TimeUnit.SECONDS)))
          }
        case "F9S_MW_HST" =>
          val collection: MongoCollection[Document] = mongoConf.database.getCollection(collectionName)
          val polCodes = targetTopics.select("polCode")
            .rdd.map(r => r(0).asInstanceOf[String].split("\\|").map(_.toString)).collect().flatten.toList
          val podCodes = targetTopics.select("podCode")
            .rdd.map(r => r(0).asInstanceOf[String].split("\\|").map(_.toString)).collect().flatten.toList
          val baseYearWeeks = targetTopics.select("baseYearWeek")
            .rdd.map(r => r(0).asInstanceOf[String].split("\\|").map(_.toString)).collect().flatten.toList
          for (i <- polCodes.indices) {
            val observable: Observable[DeleteResult] = collection
              .deleteMany(Filters.and(Filters.in("polCode", polCodes(i)), Filters.in("podCode", podCodes(i)), Filters.in("baseYearWeek", baseYearWeeks(i))))
            val observer: Observer[DeleteResult] = new Observer[DeleteResult] {
              override def onNext(result: DeleteResult): Unit = println("Delete")

              override def onError(e: Throwable): Unit = println("Failed")

              override def onComplete(): Unit = println("Completed")
            }
            observable.subscribe(observer)
            System.out.println(Await.result(observable.toFuture, Duration(10, TimeUnit.SECONDS)))
          }
        case "F9S_MW_WKDETAIL" =>
          val collection: MongoCollection[Document] = mongoConf.database.getCollection(collectionName)
          val polCodes = targetTopics.select("polCode")
            .rdd.map(r => r(0).asInstanceOf[String].split("\\|").map(_.toString)).collect().flatten.toList
          val podCodes = targetTopics.select("podCode")
            .rdd.map(r => r(0).asInstanceOf[String].split("\\|").map(_.toString)).collect().flatten.toList
          val baseYearWeeks = targetTopics.select("baseYearWeek")
            .rdd.map(r => r(0).asInstanceOf[String].split("\\|").map(_.toString)).collect().flatten.toList
          for (i <- polCodes.indices) {
            val observable: Observable[DeleteResult] = collection
              .deleteMany(Filters.and(Filters.in("polCode", polCodes(i)), Filters.in("podCode", podCodes(i)), Filters.in("baseYearWeek", baseYearWeeks)))
            val observer: Observer[DeleteResult] = new Observer[DeleteResult] {
              override def onNext(result: DeleteResult): Unit = println("Delete")

              override def onError(e: Throwable): Unit = println("Failed")

              override def onComplete(): Unit = println("Completed")
            }
            observable.subscribe(observer)
            System.out.println(Await.result(observable.toFuture, Duration(10, TimeUnit.SECONDS)))
          }
      }


    }

    ftrData = ftrSourceCaching(2)
    ftrDataSnapshot = ftrSourceCaching(1)
    val tableNm = ftrDataSnapshot.map(_._1)
    f9sId = ftrGetId_fromSource(ftrDataSnapshot)
    dbId = ftrGetId_fromSource(ftrData)
    mdmCryr = ftrData.find(_._1 == "MDM_CRYR").get._2
      .select(col("CRYR_CD").as("carrierCode"), col("CRYR_NM").as("carrierName"))

    while (i - i == 0) {
      println("//////////////////////Job " + i.toString + " Started//////////////////////")
      ///// 1) check if there is any updated data in target DB
      i match {
        //////////////////////////////////////////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////////////////////////////////////////
        case 0 =>
          //////////////////////////////////////////////////////////////////////////////////////////////////////////
          //////////////////////////////////////////////////////////////////////////////////////////////////////////
          ///////////////////////////////// Code Playground --> set appConf.jobIdx => 0 ////////////////////////////
          f9sStatsRaw = F9S_STATS_RAW(spark).stats_raw(ftrDataSnapshot, mdmCryr).cache
          f9sMwSum = F9S_MW_SUM(spark, currentWk).mw_sum(f9sStatsRaw).cache
          f9sMwSum = f9sMwSum.sample(0.1)
          f9sMwSum.toJSON.foreach(x => ArtemisProducer.sendMessage("/topic/test", x.mkString))
          Thread.sleep(1000 * 10)
        //////////////////////////////////////////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////////////////////////////////////////
        case 1 =>
          //////////////////////////////////////////////////////////////////////////////////////////////////////////
          //////////////////////////////////////////////////////////////////////////////////////////////////////////
          //                                  Cold Run -> get all data and update all                             //
          f9sStatsRaw = F9S_STATS_RAW(spark).stats_raw(ftrDataSnapshot, mdmCryr).cache
          f9sMwSum = F9S_MW_SUM(spark, currentWk).mw_sum(f9sStatsRaw).cache
          f9sMwHst = F9S_MW_HST(spark).mw_hst(f9sStatsRaw).cache
          f9sMwWkDetail = F9S_MW_WKDETAIL(spark).mw_wkdetail(f9sStatsRaw).cache

          println("//////////////////////////////////////////////////////////////////////////")
          println("/////////////////////Job " + i.toString + " Finished//////////////////////")
          println("//////////////////////////////////////////////////////////////////////////")
          if (appConf().mongoSaveYesNo == "Y") {
            f9sWrite2DB(f9sMwSum, "F9S_MW_SUM", "overwrite")
            f9sWrite2DB(f9sMwHst, "F9S_MW_HST", "overwrite")
            f9sWrite2DB(f9sMwWkDetail, "F9S_MW_WKDETAIL", "overwrite")
          }

          i += 1
        //////////////////////////////////////////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////////////////////////////////////////
        case _ =>
          //////////////////////////////////////////////////////////////////////////////////////////////////////////
          //////////////////////////////////////////////////////////////////////////////////////////////////////////
          //                                               CDC function                                           //
          var f9sStatsRawUpdatedDataOnlySnapshot = spark.emptyDataFrame
          var f9sUpdatedTopicsMarketWatch = spark.emptyDataFrame
          var f9sUpdatedTopicsProductWeekDetail = spark.emptyDataFrame
          var f9sUpdatedTopicsProductDealHistory = spark.emptyDataFrame

          idList = ftrConcatIdList(ftrGetId_fromSource(ftrSourceCaching(2)), f9sId, tableNm)
          println(idList)
          ftrUpdatedDataSnapshot = CDC_SVC(spark, i, currentWk).getUpdatedData(idList, ftrSourceCaching(2))
          ftrDataSnapshot = ftrUpdatedDataSnapshot

          if (ftrUpdatedDataSnapshot.size == 5) {
            ftrUpdatedDataOnlySnapshot = CDC_SVC(spark, i, currentWk).getFtrUpdatedDataOnlySnapshot(ftrUpdatedDataSnapshot, idList)
            f9sId = CDC_SVC(spark, i, currentWk).updateF9sId(idList, ftrUpdatedDataSnapshot)
            f9sStatsRawUpdatedDataOnlySnapshot = F9S_STATS_RAW(spark).stats_raw(ftrUpdatedDataOnlySnapshot, mdmCryr)
            println(": New dealt data incoming... " + "Total " + ftrUpdatedDataOnlySnapshot.find(_._1 == "FTR_DEAL").get._2.select("DEAL_NR").distinct.count)
            println(": Total   '" + f9sStatsRawUpdatedDataOnlySnapshot.count + "'    rows are updated for market watch")

            f9sUpdatedTopicsMarketWatch = CDC_SVC(spark, i, currentWk)
              .makeMarketWatchTopicList(f9sStatsRawUpdatedDataOnlySnapshot, "f9sUpdatedTopicsMarketWatch")
            f9sUpdatedTopicsProductWeekDetail = CDC_SVC(spark, i, currentWk)
              .makeMarketWatchTopicList(f9sStatsRawUpdatedDataOnlySnapshot, "f9sUpdatedTopicsProductWeekDetail")
            f9sUpdatedTopicsProductDealHistory = CDC_SVC(spark, i, currentWk)
              .makeMarketWatchTopicList(f9sStatsRawUpdatedDataOnlySnapshot, "f9sUpdatedTopicsProductDealHistory")

            println(":  '" + f9sUpdatedTopicsMarketWatch.count + "'  " + "Topics updated for f9sUpdatedTopicsMarketWatch")
            println(":  '" + f9sUpdatedTopicsProductWeekDetail.count * 3 + "'  " + "Topics updated for f9sUpdatedTopicsProductWeekDetail")
            println(":  '" + f9sUpdatedTopicsProductDealHistory.count + "'  " + "Topics updated for f9sUpdatedTopicsProductDealHistory")

            f9sMwSum.unpersist
            f9sMwHst.unpersist
            f9sMwWkDetail.unpersist
            f9sMwSum = F9S_MW_SUM(spark, currentWk).append_mw_sum(f9sStatsRaw, f9sUpdatedTopicsMarketWatch).cache
            f9sMwHst = F9S_MW_HST(spark).append_mw_hst(f9sStatsRaw, f9sUpdatedTopicsProductDealHistory).cache
            f9sMwWkDetail = F9S_MW_WKDETAIL(spark).append_mw_wkdetail(f9sStatsRaw, f9sUpdatedTopicsProductWeekDetail)
              .sort(col("interval").desc).cache

            if (appConf().BrokerServiceYesNo == "Y") {
              ArtemisProducer.sendMessage("/topic/f9sMarketWatch", "[" + f9sMwSum.toJSON.collect().mkString(",") + "]")
              ArtemisProducer.sendMessage("/topic/f9sMarketWatch", "[" + f9sMwHst.toJSON.collect().mkString(",") + "]")
              ArtemisProducer.sendMessage("/topic/f9sMarketWatch", "[" + f9sMwWkDetail.toJSON.collect().mkString(",") + "]")
            }
            if (appConf().mongoSaveYesNo == "Y") {
              deleteMongoSeats("F9S_MW_SUM", f9sUpdatedTopicsMarketWatch)
              deleteMongoSeats("F9S_MW_WKDETAIL", f9sUpdatedTopicsProductWeekDetail)
              deleteMongoSeats("F9S_MW_HST", f9sUpdatedTopicsProductDealHistory)

              f9sWrite2DB(f9sMwSum, "F9S_MW_SUM", "append")
              f9sWrite2DB(f9sMwWkDetail, "F9S_MW_WKDETAIL", "append")
              f9sWrite2DB(f9sMwHst, "F9S_MW_HST", "append")
            }

          }
      }
    }
  }

}
