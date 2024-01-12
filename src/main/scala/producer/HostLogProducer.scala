package producer

import models.{HostLog, UserDetail}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory
import util.{JsonUtil, JsonUtils}

import java.text.SimpleDateFormat
import java.util.{Date, Properties, Random, UUID}


object HostLogProducer {
  val logger = LoggerFactory.getLogger(this.getClass)

  private val random = new Random()

  var i = 0L

  def getMetricValue(): Int = {

    getMetricItem() match {
      case "cpu" => random.nextInt(100)
      case "memory" => random.nextInt(64)
      case _ => random.nextInt(10000)
    }

  }

  def getMetricItem(): String = {
    if (random.nextInt(100) % 3 == 0) "cpu"
    else if (random.nextInt(100) % 3 == 1) "memory"
    else "disk"
  }

  def getKafkaConfig = {
    val props = new Properties()
    props.put("bootstrap.servers", "192.168.100.231:9092")
//    props.put("bootstrap.servers", "pro-kafka-01:9092,pro-kafka-02:9092,pro-kafka-03:9092")
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
    val topic = "t_host_log"

    while (true) {
      /**
       * JSONObject uses LinkedHashMap(default LinkedHashMap<Object, Object>)
       * to store data
       * JSONObject implements Serializable
       */
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//      val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      //      val dateFormat =  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:00.000'Z'");
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
      val data = HostLog(
  System.currentTimeMillis(),
        getMetricItem(),
        getMetricValue()
      )


      val record = new ProducerRecord[String, String](topic, JsonUtil.toJson(data))
//      val record = new ProducerRecord[String, String](topic, JsonUtils.writeValueAsString(data))

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
