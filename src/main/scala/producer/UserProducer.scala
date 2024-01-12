package producer

import java.text.SimpleDateFormat
import java.util.{Date, Properties, Random, UUID}

import models.{BehaviorData, UserData, UserInfo}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.codehaus.jettison.json.JSONObject
import org.slf4j.LoggerFactory
import util.JsonUtil


object UserProducer {
  val logger = LoggerFactory.getLogger(this.getClass)

  private val users = Array(
    "4A4D769EB9679C054DE81B973ED5D768", "8dfeb5aaafc027d89349ac9a20b3930f",
    "011BBF43B89BFBF266C865DF0397AA71", "f2a8474bf7bd94f0aabbd4cdd2c06dcf",
    "068b746ed4620d25e26055a9f804385f", "97edfc08311c70143401745a03a50706",
    "d7f141563005d1b5d0d3dd30138f3f62", "c8ee90aade1671a21336c721512b817a",
    "6b67c8c700427dee7552f81f3228c927", "a95f22eabc4fd4b580c011a3161a9d9d"
  )

  private var pointer = -1
  def getUserId(): String = {
    pointer += 1
    if (pointer >= users.length) pointer = 0
    users(pointer)
  }




  private val ageRandom = new Random()

  def getAge(): Int = {
    ageRandom.nextInt(100)
  }

  def getSex(): String = {
    if (ageRandom.nextInt(100) % 2 == 0) "female"
    else "male"
  }

  def getKafkaConfig = {
    val props = new Properties()
    props.put("bootstrap.servers", "cdh1:9092,cdh2:9092,cdh3:9092")
//    props.put("bootstrap.servers", "hadoop04:9092,hadoop05:9092,hadoop06:9092")
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
        val topic = "t_bs_user"
//    val topic = "t_user_f"
    //    val topic = "user_long"

    while (true) {
      /**
       * JSONObject uses LinkedHashMap(default LinkedHashMap<Object, Object>)
       * to store data
       * JSONObject implements Serializable
       */
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
//      val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      //      val dateFormat =  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:00.000'Z'");
      val date = new Date(System.currentTimeMillis());
      val jsonSchemaDate = dateFormat.format(date);

      val data = UserData(getUserId(),
        getSex(),
        getAge(),
        jsonSchemaDate
      )
//      val data = UserInfo(getUserId(),
//        getSex(),
//        getAge(),
//        jsonSchemaDate
//      )


      val record = new ProducerRecord[String, String](topic, JsonUtil.toJson(data))

      kafkaProducer
        .send(record, new Callback() {
          def onCompletion(metadata: RecordMetadata, e: Exception) {
            if (e != null)
              logger.error(s"producer send error,msg:${e.getMessage}")
          }
        })

      println("Message sent:" + JsonUtil.toJson(data))

      /** speed of producing,unit is millisecond */
      Thread.sleep(1000)
    }

  }

  def sendMessage[T](topic: String, message: T): Unit = {
    val record = new ProducerRecord[String, String](topic, JsonUtil.toJson(message))

    kafkaProducer.send(record,
      new Callback() {
        def onCompletion(metadata: RecordMetadata, e: Exception) {
          if (e != null)
            logger.error(s"producer send error,msg:${e.getMessage}")
        }
      })
  }

}
