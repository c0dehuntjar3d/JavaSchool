package sbp.school.kafka.config.transaction;

import java.time.Duration;
import sbp.school.kafka.properties.loader.PropertiesLoader;

public class KafkaProducerProperties {

    private final static String ACK_TME = "transaction.ack.timeout";

    public static Duration getAckTime() {
        return Duration.parse(PropertiesLoader.getProperty(ACK_TME));
    }

}
