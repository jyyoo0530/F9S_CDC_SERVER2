package f9s.core.artemisbroker

import javax.jms.{Connection, ConnectionFactory, Message, MessageConsumer, MessageProducer, Session, TextMessage, Topic}
import javax.naming.InitialContext
import org.apache.activemq.artemis.core.protocol.stomp.StompConnection
import org.apache.activemq.artemis.jms.client.{ActiveMQConnectionFactory, ActiveMQQueue, ActiveMQTopic}

object ArtemisProducer {
  val connectionFactory: ActiveMQConnectionFactory = new ActiveMQConnectionFactory("tcp://data.freight9.com:61616")
    .setUser("f9s")
    .setPassword("12345678")
  val connection: Connection = connectionFactory.createConnection()
  connection.start()
  val session: Session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

  def sendMessage(topicAddress: String, messageContent: String): Unit = {
    val topic: ActiveMQTopic = new ActiveMQTopic(topicAddress)
    val producer: MessageProducer = session.createProducer(topic)
    val message: TextMessage = session.createTextMessage(messageContent)

    System.out.println("Sent message: " + message.getText)
    producer.send(message)
  }


}
