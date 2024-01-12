package producer

import java.text.SimpleDateFormat
import java.util.{Date, Properties, Random, UUID}

import models.{UserData, UserDetail, UserInfo}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory
import util.JsonUtil


object UserProducer4LongId {
  val logger = LoggerFactory.getLogger(this.getClass)

  val brokers = "192.168.100.16:9092"

  var i = 0L

  val random = new Random();

  def getUserId() = {
//    if (i >= Long.MaxValue) {
//      i = 0L
//    } else {
//      i = i + 1
//    }
//
//    i
    val n = random.nextInt(5) + 2
      if(n == 5){
      2
    } else if(n == 6) {
      3
    } else {
      n
    }

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
    props.put("bootstrap.servers", brokers)
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
    val topic = "t_user_0609"
//    val topic = "t_join_phoenix"
//    val topic = "t_sync_user"

    while (true) {
      /**
       * JSONObject uses LinkedHashMap(default LinkedHashMap<Object, Object>)
       * to store data
       * JSONObject implements Serializable
       */
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
//      val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      //      val dateFormat =  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:00.000'Z'")
      val date = new Date(System.currentTimeMillis());
      val jsonSchemaDate = dateFormat.format(date);

//            val data = UserData(getUserId(),
//              getSex(),
//              getAge(),
//              jsonSchemaDate
//            )
//            val data = UserData(
//              ""+getUserId(),
//              getSex(),
//              getAge(),
//              jsonSchemaDate
//            )
      val data = UserDetail(getUserId(),
        getSex(),
        getAge(),
        System.currentTimeMillis()
      )


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
