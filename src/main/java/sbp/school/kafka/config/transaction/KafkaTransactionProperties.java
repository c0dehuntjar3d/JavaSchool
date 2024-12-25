package sbp.school.kafka.config.transaction;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.model.Transaction.TransactionType;

import sbp.school.kafka.properties.loader.PropertiesLoader;

@Slf4j
public class KafkaTransactionProperties {

    private final static String TOPIC_PROPERTY = "transaction.topic";
    private final static String TOPIC_ACK_PROPERTY = "transaction.topic.ack";

    public final static Map<String, Integer> PARTITIONS = Arrays
            .stream(TransactionType.values())
            .collect(Collectors.toMap(TransactionType::name, TransactionType::ordinal));

    public static String getTopic() {
        return PropertiesLoader.getProperty(TOPIC_PROPERTY);
    }

    public static String getAcktTopioc() {
        return PropertiesLoader.getProperty(TOPIC_ACK_PROPERTY);
    }
}
