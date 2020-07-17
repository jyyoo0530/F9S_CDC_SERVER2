package query

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

case class F9S_MW_BIDASK(var spark: SparkSession, var pathSourceFrom: String,
                         var pathParquetSave: String, var pathJsonSave: String){
  def mw_bidask(): Unit ={
    val FTR_OFER_CRYR = spark.read.parquet(pathSourceFrom+"/FTR_OFER_CRYR")
    val FTR_OFER_RTE = spark.read.parquet(pathSourceFrom+"/FTR_OFER_RTE")
    val FTR_OFER_LINE_ITEM = spark.read.parquet(pathSourceFrom+"/FTR_OFER_LINE_ITEM")
    val FTR_OFER = spark.read.parquet(pathSourceFrom+"/FTR_OFER")
    val MDM_PORT = spark.read.parquet(pathSourceFrom+"/MDM_PORT")
    // val MDM_CRYR = spark.read.parquet(folderOrigin+"/MDM_CRYR")
    lazy val leftJoinSeq1 = Seq("tradeOfferNumber", "tradeOfferChangeSeq")
    lazy val filterSeq = FTR_OFER.groupBy("OFER_NR").agg(max("OFER_CHNG_SEQ").as("OFER_CHNG_SEQ"))

    lazy val cryrSrc = filterSeq.join(FTR_OFER_CRYR,Seq("OFER_NR", "OFER_CHNG_SEQ"),"left").select("OFER_NR", "OFER_CHNG_SEQ", "OFER_CRYR_CD")
      .withColumn("tradeOfferNumber", col("OFER_NR")).drop("OFER_NR")
      .withColumn("tradeOfferChangeSeq", col("OFER_CHNG_SEQ")).drop("OFER_CHNG_SEQ")
      .withColumn("carrierCode", col("OFER_CRYR_CD")).drop("OFER_CRYR_CD")
    lazy val locationSrc = filterSeq.join(FTR_OFER_RTE,Seq("OFER_NR", "OFER_CHNG_SEQ"),"left").select("OFER_NR", "OFER_CHNG_SEQ", "OFER_REG_SEQ", "TRDE_LOC_CD", "TRDE_LOC_TP_CD")
      .withColumn("tradeOfferNumber", col("OFER_NR")).drop("OFER_NR")
      .withColumn("tradeOfferChangeSeq", col("OFER_CHNG_SEQ")).drop("OFER_CHNG_SEQ")
      .withColumn("routeRegSeq", col("OFER_REG_SEQ")).drop("OFER_REG_SEQ")
      .withColumn("locationCd", col("TRDE_LOC_CD")).drop("TRDE_LOC_CD")
      .withColumn("locationTp", col("TRDE_LOC_TP_CD")).drop("TRDE_LOC_TP_CD")
    lazy val lineSrc = filterSeq.join(FTR_OFER_LINE_ITEM,Seq("OFER_NR", "OFER_CHNG_SEQ"),"left").select("OFER_NR", "OFER_CHNG_SEQ", "BSE_YW", "OFER_PRCE", "OFER_REMN_QTY", "TRDE_CONT_TP_CD")
      .withColumn("tradeOfferNumber", col("OFER_NR")).drop("OFER_NR")
      .withColumn("tradeOfferChangeSeq", col("OFER_CHNG_SEQ")).drop("OFER_CHNG_SEQ")
      .withColumn("baseYearWeek", col("BSE_YW")).drop("BSE_YW")
      .withColumn("offerPrice", col("OFER_PRCE")).drop("OFER_PRCE")
      .withColumn("offerRemainderQty", col("OFER_REMN_QTY")).drop("OFER_REMN_QTY")
      .withColumn("containerTypeCode", col("TRDE_CONT_TP_CD")).drop("TRDE_CONT_TP_CD")
    lazy val oferSrc = filterSeq.join(FTR_OFER,Seq("OFER_NR", "OFER_CHNG_SEQ"),"left").select("OFER_NR", "OFER_CHNG_SEQ", "TRDE_MKT_TP_CD", "OFER_TP_CD", "OFER_PYMT_TRM_CD", "OFER_RD_TRM_CD", "ALL_YN")
      .withColumn("tradeOfferNumber", col("OFER_NR")).drop("OFER_NR")
      .withColumn("tradeOfferChangeSeq", col("OFER_CHNG_SEQ")).drop("OFER_CHNG_SEQ")
      .withColumn("marketTypeCode", col("TRDE_MKT_TP_CD")).drop("TRDE_MKT_TP_CD")
      .withColumn("offerTypeCode", col("OFER_TP_CD")).drop("OFER_TP_CD")
      .withColumn("paymentTermCode", col("OFER_PYMT_TRM_CD")).drop("OFER_PYMT_TRM_CD")
      .withColumn("rdTermCode", col("OFER_RD_TRM_CD")).drop("OFER_RD_TRM_CD")
      .withColumn("allYn", col("ALL_YN")).drop("ALL_YN")
    lazy val mdmPortSrc = MDM_PORT.select("locCd", "locNm").withColumn("locationCd", col("locCd")).drop("locCd").distinct



    lazy val companyList = cryrSrc.groupBy("tradeOfferNumber","tradeOfferChangeSeq").agg(collect_list("carrierCode").as("companyCodes"), first("carrierCode").as("carrierCode"), count("carrierCode").as("carrierCount"))

    lazy val bidaskList = lineSrc.join(oferSrc, leftJoinSeq1, "left").join(companyList, leftJoinSeq1, "left")

    lazy val calItem = bidaskList.groupBy("tradeOfferNumber", "tradeOfferChangeSeq").agg(avg("offerPrice").as("avgPrice"), avg("offerRemainderQty").as("avgQty"), max("baseYearWeek").as("maxYearWeek"), min("baseYearWeek").as("minYearWeek"), max("offerPrice").as("maxPrice"), min("offerPrice").as("minPrice"))
    lazy val srcOutput = bidaskList.join(calItem, Seq("tradeOfferNumber", "tradeOfferChangeSeq"), "left").drop("offerPirce", "offerRemainderQty")

    lazy val locationPol = locationSrc.filter(col("locationTp") === "02").join(mdmPortSrc,Seq("locationCd"),"left")
      .withColumn("polName", col("locNm")).drop("locNm")
      .withColumn("polCode", col("locationCd")).drop("locationCd", "locationTp", "routeRegSeq")

    lazy val polCount = locationPol.groupBy("tradeOfferNumber", "tradeOfferChangeSeq").agg(count("polCode").as("polCount"))

    lazy val polList = locationPol.join(polCount, leftJoinSeq1, "left")

    lazy val locationPod = locationSrc.filter(col("locationTp") === "03").join(mdmPortSrc,Seq("locationCd"),"left")
      .withColumn("podName", col("locNm")).drop("locNm")
      .withColumn("podCode", col("locationCd")).drop("locationCd", "locationTp", "routeRegSeq")

    lazy val podCount = locationPod.groupBy("tradeOfferNumber", "tradeOfferChangeSeq").agg(count("podCode").as("podCount"))

    lazy val podList = locationPod.join(podCount, leftJoinSeq1, "left")

    val finalSrc = polList.join(podList, leftJoinSeq1, "left").withColumn("headPolCode", col("polCode"))
      .withColumn("headpodCode", col("podCode"))
      .withColumn("headPolName", col("polName")).drop("polName")
      .withColumn("headPodName", col("podName")).drop("podName")
      .join(srcOutput, leftJoinSeq1, "left").distinct

    val F9S_MW_BIDASK = finalSrc.groupBy("marketTypeCode", "offerTypeCode", "baseYearWeek", "paymentTermCode", "rdTermCode", "containerTypeCode", "polCode", "podCode")
      .agg(collect_list(struct("tradeOfferNumber", "tradeOfferChangeSeq", "carrierCode", "carrierCount", "headPolCode", "headPodCode", "headPolName", "headPodName", "polCount", "podCount", "avgQty", "avgPrice","allYn", "minYearWeek", "maxYearWeek", "maxPrice", "minPrice")).as("Cell"))

    lazy val mwidx = F9S_MW_BIDASK.select("marketTypeCode", "rdTermCode", "containerTypeCode", "paymentTermCode", "polCode", "podCode", "baseYearWeek","offerTypeCode").distinct.withColumn("writeIdx", concat(col("marketTypeCode"), col("rdTermCode"), col("containerTypeCode"), col("paymentTermCode"), col("polCode"), col("podCode"), col("baseYearWeek"), col("offerTypeCode"))).withColumn("idx", row_number.over(Window.orderBy(col("writeIdx")))).withColumn("rte_idx", concat(col("polCode"), col("podCode")))
    lazy val idx = mwidx.collect()
    lazy val marketTypeCode = mwidx.select("marketTypeCode").collect().map(_(0).toString)
    lazy val paymentTermCode = mwidx.select("paymentTermCode").collect().map(_(0).toString)
    lazy val rdTermCode = mwidx.select("rdTermCode").collect().map(_(0).toString)
    lazy val containerTypeCode = mwidx.select("containerTypeCode").collect().map(_(0).toString)
    lazy val rte_idx = mwidx.select("rte_idx").collect().map(_(0).toString)
    lazy val baseYearWeek = mwidx.select("baseYearWeek").collect().map(_(0).toString)
    lazy val offerTypeCode = mwidx.select("offerTypeCode").collect().map(_(0).toString)

    lazy val tgData = F9S_MW_BIDASK.withColumn("rte_idx", concat(col("polCode"), col("podCode")))

    for (i <- idx.indices){
      tgData.filter(
        col("marketTypeCode") === marketTypeCode(i) &&
          col("paymentTermCode") === paymentTermCode(i) &&
          col("rdTermCode") === rdTermCode(i) &&
          col("containerTypeCode") === containerTypeCode(i) &&
          col("rte_idx") === rte_idx(i) &&
          col("baseYearWeek") === baseYearWeek(i) &&
          col("offerTypeCode") === offerTypeCode(i)
      )
        .drop("rte_idx","writeIdx")
        .write.mode("append").json(pathJsonSave+"/F9S_MW_BIDASK"+"/"+marketTypeCode(i)+"/"+offerTypeCode(i)+ "/" + baseYearWeek(i)+"/"+paymentTermCode(i)+"/"+rdTermCode(i)+"/"+containerTypeCode(i)+"/"+rte_idx(i))
    }

    F9S_MW_BIDASK.write.mode("append").parquet(pathParquetSave+"/F9S_MW_BIDASK")

  }

}
