package consumer
import org.apache.kafka.clients.consumer.KafkaConsumer

import java.time.Duration
import java.util.{Collections, Properties}
import scala.collection.JavaConverters._

object KafkaConsumerDemo {

  def main(args: Array[String]): Unit = {

    val props = new Properties()

    // 必须设置的属性
    props.put("bootstrap.servers", "192.168.100.171:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "group1")

    // 可选设置属性
    props.put("enable.auto.commit", "true")
    // 自动提交offset,每1s提交一次
    props.put("auto.commit.interval.ms", "1000")
    props.put("auto.offset.reset", "earliest ")
    props.put("client.id", "zy_client_id")
    val consumer = new KafkaConsumer[String, String](props)
    // 订阅test1 topic
//    consumer.subscribe(Collections.singletonList("t_host"))
    consumer.subscribe(Collections.singletonList("t_host_log_1216_1"))

    while (true) { //  从服务器开始拉取数据
      val records = consumer.poll(Duration.ofMillis(100)).asScala

      records.foreach(record =>

        println(
          s"""
             |topic: ${record.topic()},
             |partition: ${record.partition()},
             |offset: ${record.offset()},
             |key: ${record.key()},
             |value: ${record.value()},
             |""".stripMargin)

      )
    }
  }

}
