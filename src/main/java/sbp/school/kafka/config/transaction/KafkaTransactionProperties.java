package sbp.school.kafka.config.transaction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.config.KafkaProperties;
import sbp.school.kafka.model.Transaction.TransactionType;
import sbp.school.kafka.properties.loader.PropertiesLoader;

@Slf4j
public class KafkaTransactionProperties {
    
    private final static String TOPIC_PROPERTY = "transaction.topic";

    public final static Map<String, Integer> PARTITIONS = Arrays
        .stream(TransactionType.values())
        .collect(Collectors.toMap(TransactionType::name, TransactionType::ordinal));

    public static String getTopic() {
        try {
            Properties fileProps = PropertiesLoader.loadProperties(KafkaProperties.APPLICATION_PROPERTIES_FILE);
            return fileProps.getProperty(TOPIC_PROPERTY);
        } catch (IOException e) {
            log.error(
                "error loading for: {}, {}. {}",
                KafkaProperties.APPLICATION_PROPERTIES_FILE,
                TOPIC_PROPERTY,
                e.getMessage()
            );
            throw new RuntimeException(e);
        }
    }

}
