package f9s

import java.util.Properties

import org.mongodb.scala.{MongoClient, MongoDatabase}

case class jdbcConf() {
  val prop = new Properties()
  prop.put("user", "ftradm")
  prop.put("password", "12345678")
  val url = "jdbc:mysql://opus365-dev01.cbqbqnguxslu.ap-northeast-2.rds.amazonaws.com:3306"
}

object mongoConf {
  private val uri: String = "mongodb://ec2-13-209-15-68.ap-northeast-2.compute.amazonaws.com:27017"
  System.setProperty("org.mongodb.async.type", "netty")
  private val mongoClient: MongoClient = MongoClient(uri)
  val database: MongoDatabase = mongoClient.getDatabase("f9s")
}
