package producer

import java.text.SimpleDateFormat
import java.util.{Date, Properties, Random, UUID}

import models.{BehaviorData, Diamond, DiamondStr}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.codehaus.jettison.json.JSONObject
import org.slf4j.LoggerFactory
import producer.DiamondProducer.random
import util.JsonUtil


object DiamondProducer {
  val logger = LoggerFactory.getLogger(this.getClass)

  private val users = Array(
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10
  )

  private var pointer = -1

  def getUserId() = {
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


  def getItemType() = {
    random.nextInt(100) % 7
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
    val topic = "diamond"

    while (true) {
      /**
       * JSONObject uses LinkedHashMap(default LinkedHashMap<Object, Object>)
       * to store data
       * JSONObject implements Serializable
       */

      val dateFormat =  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:00.000'Z'")
      val date = new Date(System.currentTimeMillis());
      val jsonSchemaDate = dateFormat.format(date);

      val data = DiamondStr(
        jsonSchemaDate,
        getUserId(),
        getIsAdd(),
        getSourceType(),
        getItemType(),
        getValue()
      )
//      val data = Diamond(
//        System.currentTimeMillis(),
//        getUserId(),
//        getIsAdd(),
//        getSourceType(),
//        getItemType(),
//        getValue()
//      )

      val record = new ProducerRecord[String, String](topic, JsonUtil.toJson(data))
      //      val userBehavior = getBehaviorData()
      //      val record = new ProducerRecord[String, String](topic, userBehavior.toString())

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
