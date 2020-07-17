package query


trait F9S_CMMN {
  import org.apache.spark.sql.SparkSession
  var spark: SparkSession
  var pathSourceFrom: String
  var pathParquetSave: String
  var pathJsonSave: String
  var currentWk: String
}
