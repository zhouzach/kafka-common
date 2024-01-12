package producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import model.HostLogWithID;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.JsonUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class DuplicateHostLogProducer {
    private static final Logger logger = LoggerFactory.getLogger(DuplicateHostLogProducer.class);

    private static Random random = new Random();

    public static void main(String[] args) throws JsonProcessingException, InterruptedException {
        Map<String, Object> props = new HashMap<>(16);

//        String brokers = "192.168.100.168:9092,192.168.100.195:9092,192.168.100.196:9092";
//        String brokers = "192.168.1.242:9093";
        String brokers = "192.168.1.242:9092";
//        String brokers = "dm-host118:9092,dm-host211:9092,dm-host214:9092";
//        String brokers = "192.168.100.16:9092";
//        String topic = "t_host_id_0614_1";
        String topic = "t_host5";

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

//        while (true) {
        HostLogWithID hostLog1 = new HostLogWithID();
        hostLog1.setId(1L);
//        hostLog.setCreatedTime(System.currentTimeMillis());
        hostLog1.setCreatedTime(1655964596023L);
        hostLog1.setMetricItem("cpu");
        hostLog1.setMetricValue(68);
        try {
            String data = JsonUtils.writeValueAsString(hostLog1);
            kafkaProducer
                    .send(new ProducerRecord<>(topic, null, data), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception != null)
                                logger.error(exception.getMessage(), exception);
                        }
                    });
            System.out.println(data);
            Thread.sleep(5000);
        } catch (JsonProcessingException | InterruptedException e) {
            e.printStackTrace();
        }


        HostLogWithID hostLog2 = new HostLogWithID();
        hostLog2.setId(2L);
        hostLog2.setCreatedTime(1655964596023L);
        hostLog2.setMetricItem("memory");
        hostLog2.setMetricValue(32);
        try {
            String data = JsonUtils.writeValueAsString(hostLog2);
            kafkaProducer
                    .send(new ProducerRecord<>(topic, null, data), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception != null)
                                logger.error(exception.getMessage(), exception);
                        }
                    });
            System.out.println(data);
            Thread.sleep(5000);
        } catch (JsonProcessingException | InterruptedException e) {
            e.printStackTrace();
        }

        HostLogWithID hostLog3 = new HostLogWithID();
        hostLog3.setId(3L);
        hostLog3.setCreatedTime(1655964596023L);
        hostLog3.setMetricItem("disk");
        hostLog3.setMetricValue(500);
        try {
            String data = JsonUtils.writeValueAsString(hostLog3);
            kafkaProducer
                    .send(new ProducerRecord<>(topic, null, data), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception != null)
                                logger.error(exception.getMessage(), exception);
                        }
                    });
            System.out.println(data);
            Thread.sleep(5000);
        } catch (JsonProcessingException | InterruptedException e) {
            e.printStackTrace();
        }

        HostLogWithID hostLog5 = new HostLogWithID();
        hostLog5.setId(5L);
        hostLog5.setCreatedTime(1655964596023L);
        hostLog5.setMetricItem("memory");
        hostLog5.setMetricValue(64);
        try {
            String data = JsonUtils.writeValueAsString(hostLog5);
            kafkaProducer
                    .send(new ProducerRecord<>(topic, null, data), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception != null)
                                logger.error(exception.getMessage(), exception);
                        }
                    });
            System.out.println(data);
            Thread.sleep(5000);
        } catch (JsonProcessingException | InterruptedException e) {
            e.printStackTrace();
        }

        HostLogWithID hostLog6 = new HostLogWithID();
        hostLog6.setId(1L);
        hostLog6.setCreatedTime(1655964597023L);
        hostLog6.setMetricItem("cpu");
        hostLog6.setMetricValue(68);
        try {
            String data = JsonUtils.writeValueAsString(hostLog6);
            kafkaProducer
                    .send(new ProducerRecord<>(topic, null, data), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception != null)
                                logger.error(exception.getMessage(), exception);
                        }
                    });
            System.out.println(data);
            Thread.sleep(5000);
        } catch (JsonProcessingException | InterruptedException e) {
            e.printStackTrace();
        }


    }

    public static String getMetricItem() {
        if (random.nextInt(100) % 3 == 0) return "cpu";
        else if (random.nextInt(100) % 3 == 1) return "memory";
        else return "disk";
    }

    public static Integer getMetricValue() {
        switch (getMetricItem()) {
            case "cpu":
                return random.nextInt(100);
//                break; //可选
            case "memory":
                return random.nextInt(64);
            case "disk":
                return random.nextInt(2000);
            default: //可选
                return 0;
        }

    }

    public static Long getID() {
        switch (getMetricItem()) {
            case "cpu":
                return 2L;
//                break; //可选
            case "memory":
                return 3L;
            case "disk":
                return 4L;
            default: //可选
                return 0L;
        }

    }
}
