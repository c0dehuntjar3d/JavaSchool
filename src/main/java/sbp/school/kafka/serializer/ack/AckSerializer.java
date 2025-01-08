package sbp.school.kafka.serializer.ack;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.model.Ack;

import java.nio.charset.StandardCharsets;

@Slf4j
public class AckSerializer implements Serializer<Ack> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, Ack data) {
        if (data == null) {
            log.error("Ack is null for topic: {}", topic);
            return null;
        }

        try {
            String value = mapper.writeValueAsString(data);
            return value.getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.error("Serialization error for Ack: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
