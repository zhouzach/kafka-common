package producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import model.HostLogWithID;
import model.WebLog;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.JsonUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class WebLogProducer {
    private static final Logger logger = LoggerFactory.getLogger(WebLogProducer.class);

    private static Random random = new Random();

    public static void main(String[] args) {
        Map<String, Object> props = new HashMap<>(16);

//        String brokers = "192.168.100.168:9092,192.168.100.195:9092,192.168.100.196:9092";
//        String brokers = "192.168.1.242:9093";
        String brokers = "192.168.1.242:9092";
//        String brokers = "dm-host118:9092,dm-host211:9092,dm-host214:9092";
//        String brokers = "192.168.100.16:9092";
//        String topic = "t_host_id_0614_1";
        String topic = "t_web_3";

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        //linger.ms的时间间隔批量发送消息呢
        props.put(ProducerConfig.LINGER_MS_CONFIG, 3000);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024);
        //该参数指定了在调用send()方法或使用partitionsFor()方法获取元数据时生产者的阻塞时间，当生产者的缓冲区已满，或没有可用的元数据时，这些方法就会阻塞。
        // 在阻塞时间达到max.block.ms时，生产者抛出超时异常
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put("compression.type", "none");
        props.put("client.id", UUID.randomUUID().toString());
        props.put("acks", "1");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        while (true) {
            WebLog webLog = new WebLog();
            webLog.setId(getID());
            webLog.setCreatedTime(System.currentTimeMillis());
            webLog.setLog(getLog());

            try {
                String data = JsonUtils.writeValueAsString(webLog);
                kafkaProducer
                        .send(new ProducerRecord<>(topic, null, data), new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata metadata, Exception exception) {
                                if (exception != null)
                                    logger.error(exception.getMessage(), exception);
                            }
                        });
                System.out.println(data);
                Thread.sleep(200);
            } catch (JsonProcessingException | InterruptedException e) {
                e.printStackTrace();
            }

        }

    }

    public static String getLog(){
        String ip = "";
        String method = "";
        String uri = "";
        String bytes = "";
        String duration = "";

        String separator = " ";

        if (random.nextInt(100) % 3 == 0) {
            ip = "127.0.0.1";
            method = "GET";
            uri = "/index.html";
            bytes = "15824";
            duration = "0.043";
        }
        else if (random.nextInt(100) % 3 == 1) {
            ip = "192.168.0.1";
            method = "POST";
            uri = "/login.html";
            bytes = "16820";
            duration = "0.053";
        }
        else {
            ip = "192.168.0.2";
            method = "PUT";
            uri = "/logout.html";
            bytes = "17820";
            duration = "0.678";
        }
        return ip + separator + method + separator + uri + separator + bytes + separator + duration;
    }


    private static Long totalCount = 0L;
    public static Long getID() {
//        return random.nextLong();
        return ++totalCount;
    }
}
