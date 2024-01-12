package serialization;

//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * https://stackoverflow.com/questions/58644549/how-to-implement-flinkkafkaproducer-serializer-for-kafka-2-2
 *
 * sending POJO to Kafka
 */
public class ObjectSerializationSchema implements KafkaSerializationSchema<Pojo> {
    private String topic;
    private ObjectMapper mapper;

    public ObjectSerializationSchema(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Pojo obj, Long timestamp) {
        byte[] b = null;
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        try {
            b= mapper.writeValueAsBytes(obj);
        } catch (JsonProcessingException e) {
            // TODO
        }
        return new ProducerRecord<byte[], byte[]>(topic, b);
    }
}
