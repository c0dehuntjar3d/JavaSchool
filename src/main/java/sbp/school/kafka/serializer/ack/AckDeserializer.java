package sbp.school.kafka.serializer.ack;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.model.Ack;

@Slf4j
public class AckDeserializer implements Deserializer<Ack> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Ack deserialize(String topic, byte[] data) {
        if (data == null) {
            log.error("Data is null for topic: {}", topic);
            return null;
        }

        try {
            return mapper.readValue(data, Ack.class);
        } catch (Exception e) {
            log.error("Deserialization error for Ack: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
