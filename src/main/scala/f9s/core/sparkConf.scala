package f9s.core

import org.apache.spark.SparkConf

case class sparkConf() {
  val master = "local[*]"
  //    val master = "spark://192.168.0.6:7077"
  val appName = "MyApp"
  val conf: SparkConf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)
    //      .set("spark.driver.allowMultipleContexts", "false")
    .set("spark.ui.enabled", "true")
    .set("spark.ui.port", "5555")
    //    .set("spark.speculation","false")
    //      .set("spark.driver.cores", "2")
    //      .set("spark.driver.memory", "12g")
    .set("spark.executor.memoryOverhead", "1g")
    .set("spark.sql.hive.convertMetastoreParquet", "false")
    //      .set("spark.cores.max", "4")
    //      .set("spark.executor.memory", "10g")
    //      .set("spark.speculation", "true")
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

    ////////Delta Lake/////
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog_spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

  ////// GPU SETTING ////////
//      .set("spark.rapids.sql.incompatibleOps.enabled", "true")
//      .set("spark.executor.resource.gpu.amount", "1")
//      .set("spark.rapids.sql.concurrentGpuTasks", "2")
//      .set("spark.sql.files.maxPartitionBytes", "512m")
//      .set("spark.kryo.registrator", "com.nvidia.spark.rapids.GpuKryoRegistrator")
//      .set("spark.rapids.sql.explain", "true")
//      .set("spark.plugins", "com.nvidia.spark.SQLPlugin")
//      .set("spark.task.resource.gpu.amount", "0.125")  ////don't use yet !!
}
