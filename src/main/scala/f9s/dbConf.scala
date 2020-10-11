package f9s

import java.util.Properties

import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.netty.channel.unix.Socket
import javax.jms.{Connection, DeliveryMode, Destination, MessageProducer, Session, TextMessage}
import org.apache.hadoop.conf.Configuration
import org.mongodb.scala.{MongoClient, MongoDatabase}
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory

object jdbcConf {
  val prop = new Properties()
  prop.put("user", "ftradm")
  prop.put("password", "12345678")
  val url = "jdbc:mysql://opus365-dev01.cbqbqnguxslu.ap-northeast-2.rds.amazonaws.com:3306"
}

object mongoConf {
  //  private val uri: String = "mongodb://ec2-13-209-15-68.ap-northeast-2.compute.amazonaws.com:27017"
  private val uri: String = "mongodb://f9s:12345678@data.freight9.com:27017/f9s?authSource=f9s&readPreference=primary"
  System.setProperty("org.mongodb.async.type", "netty")
  private val mongoClient: MongoClient = MongoClient(uri)
  val database: MongoDatabase = mongoClient.getDatabase("f9s")
  val sparkMongoUri: String = "mongodb://f9s:12345678@data.freight9.com:27017/f9s?authSource=f9s&readPreference=primary"
}

object redisConf {
  val redisClient: RedisClient = RedisClient.create("redis://localhost/0")
  val connection: StatefulRedisConnection[String, String] = redisClient.connect()
}

object hadoopConf {
  val hadoopPath = "hdfs://data.freight9.com:9000/"
  val HADOOP_USER = "root"
  val conf = new Configuration();
  conf.set("hadoop.job.ugi", HADOOP_USER)
}

object artemisConf {
  // create Connection
  val connectionFactory: ActiveMQConnectionFactory = new ActiveMQConnectionFactory("tcp://data.freight9.com:61616")
    .setUser("f9s")
    .setPassword("12345678")
  val connection: Connection = connectionFactory.createConnection()
  connection.start()
  // create Session
  val session: Session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

  def runArtemisProducer(topic: String, msg: String): Unit = {

    // create Destination ( Topic or Queue )
    val destination: Destination = session.createTopic(topic)

    // create a messageProducer from the Session to the Topic or Queue
    val producer: MessageProducer = session.createProducer(destination)
    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT)

    // create messages
    val message: TextMessage = session.createTextMessage(msg)

    // tell the producer to send the message
    System.out.println("Sent message: " + message.hashCode() + " : " + Thread.currentThread().getName);
    producer.send(message)
  }

  def killArtemisProducer(): Unit = {
    // Clean up
    session.close()
    connection.close()
  }

}
