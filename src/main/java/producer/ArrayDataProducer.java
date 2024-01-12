package producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import model.ArrayData;
import model.HostLogWithID;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.JsonUtils;

import java.util.*;

public class ArrayDataProducer {
    private static final Logger logger = LoggerFactory.getLogger(ArrayDataProducer.class);

    private static Random random = new Random();

    public static void main(String[] args) {
        Map<String, Object> props = new HashMap<>(16);

//        String brokers = "192.168.100.168:9092,192.168.100.195:9092,192.168.100.196:9092";
//        String brokers = "192.168.1.242:9092";
        String brokers = "192.168.100.231:9092";
//        String brokers = "dm-host118:9092,dm-host211:9092,dm-host214:9092";
//        String brokers = "192.168.100.16:9092";
        String topic = "t_array823";
//        String topic = "t_host_log_727";

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 3000);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put("client.id", UUID.randomUUID().toString());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        while (true) {
            ArrayData arrayData = new ArrayData();
            List<Map<String, Object>> cpus = new ArrayList<>();
            Map<String, Object> cpu = new HashMap<>();
            cpu.put("cup_id", 1);
            cpu.put("cup_value", 0.1);
            cpus.add(cpu);

            Map<String, Object> cpu2 = new HashMap<>();
            cpu2.put("cup_id", 2);
            cpu2.put("cup_value", 0.2);
            cpus.add(cpu2);
            arrayData.setCpus(cpus);

            Map<String, Object> people = new HashMap<>();
            people.put("name", "lily");
            people.put("age", 18);
            arrayData.setPeople(people);
            arrayData.setId(getID());

            try {
                String data = JsonUtils.writeValueAsString(arrayData);
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


    public static Long getID() {

        return new Long(random.nextInt(100));
    }
}
