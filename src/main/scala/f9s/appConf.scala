package f9s
import org.apache.log4j.{Level, Logger} //// 1) Runnning Mode////  ------------------> configuration 대상

case class appConf() {

  /// settings ///
  val sparkLogger = "OFF"     /// "OFF" or "FATAL" or "ALL"
  val pathMode = "OPERATION"  /// "TEST" or "OPERATION"
  val jobIdx = 1              /// "1" for normal operating, "2" for the mode except Cold-Run

  val rootPath = new java.io.File(".").getCanonicalPath + "/src/main/resources/"

  val folderOrigin = "file://" + rootPath + "OriginSource/"
  val folderStats = "file://" + rootPath + "stats/"
  val folderJSON = "file://" + rootPath + "JSON/"

  val folderOriginTest = "file://" + rootPath + "test/OriginSource/"
  val folderStatsTest = "file://" + rootPath + "test/stats/"
  val folderJSONTest = "file://" + rootPath + "test/JSON/"

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
}

