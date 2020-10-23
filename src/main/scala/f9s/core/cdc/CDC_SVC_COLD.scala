package f9s.core.cdc

import f9s.{appConf, hadoopConf, jdbcConf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

case class CDC_SVC_COLD(var spark: SparkSession,
                        var iterIdx: Int,
                        var currentWk: String) {
  val filePath: String = appConf().dataLake match {
    case "file" => appConf().folderOrigin
    case "hadoop" => hadoopConf.hadoopPath
  }

  val list2chk: List[String] = appConf().list2chk

  def getData: List[(String, DataFrame)] = {
    var ftrReadData = List[(String, DataFrame)]() //// create Empty return data
    // Update Origin Source
        val list2chk2 = list2chk ::: List("MDM_PORT", "MDM_CRYR")
        for (i <- list2chk2.indices) {
          ftrReadData = ftrReadData :::
            List((list2chk2(i), spark.read.jdbc(jdbcConf.url, "ftr." + list2chk2(i), jdbcConf.prop)))
        }
    ftrReadData
  }
}
