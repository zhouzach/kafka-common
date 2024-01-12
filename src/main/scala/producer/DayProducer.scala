package producer

import java.text.SimpleDateFormat
import java.util.{Date, Properties, Random, UUID}

import models.{DayInfo, DiamondStr}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory
import util.JsonUtil


object DayProducer {
  val logger = LoggerFactory.getLogger(this.getClass)

  private val users = Array(
//    "glamour", "diamond",
//    "new_user", "new_anchor",
//    "active_user", "recharge",
//    "online_anchor", "online_user",
    "withdraw", "live"
  )

  private var pointer = -1
  def getCatalog(): String = {
    pointer += 1
    if (pointer >= users.length) pointer = 0
    users(pointer)
  }

  private val random = new Random()

  def getValue(): Int = {
    random.nextInt(10)
  }

  def getIsAdd() = {
    if (random.nextInt(10) % 2 == 0) 0
    else 1
  }

  def getSourceType() = {
    random.nextInt(100) % 4
  }


  def getKafkaConfig = {
    val props = new Properties()
    props.put("bootstrap.servers", "cdh1:9092,cdh2:9092,cdh3:9092")
    props.put("compression.type", "none")
    props.put("retries", "0")
    props.put("client.id", UUID.randomUUID.toString)
    props.put("linger.ms", "50")
    props.put("batch.size", "1")
    props.put("acks", "1")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

  val kafkaProducer = new KafkaProducer[String, String](getKafkaConfig)

  def main(args: Array[String]): Unit = {
    val topic = "day"

    while (true) {
      val data = DayInfo(
        System.currentTimeMillis(),
        getCatalog(),
        getValue()
      )

      val record = new ProducerRecord[String, String](topic, JsonUtil.toJson(data))

      kafkaProducer
        .send(record, new Callback() {
          def onCompletion(metadata: RecordMetadata, e: Exception) {
            if (e != null)
              logger.error(s"producer send error,msg:${e.getMessage}")
          }
        })

//      println("Message sent:" + JsonUtil.toJson(data))
      println(JsonUtil.toJson(data)+",")

      /** speed of producing,unit is millisecond */
      Thread.sleep(1000)
    }

  }


}
