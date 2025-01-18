package sbp.school.kafka.config;

import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.properties.loader.PropertiesLoader;

@Slf4j
public class KafkaProperties {
    public static final String APPLICATION_PROPERTIES_FILE = "application.properties";
    public static final String PRODUCER_ID_PARAM = "Producer-id";

    public static Set<String> producerPropsSet = Set.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            ProducerConfig.PARTITIONER_CLASS_CONFIG,
            ProducerConfig.ACKS_CONFIG,
            ProducerConfig.COMPRESSION_TYPE_CONFIG);

    public static Set<String> consumerPropsSet = Set.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            ConsumerConfig.GROUP_ID_CONFIG,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
            ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG);

    public static Properties getConsumerKafkaProperties() {
        Properties fileProps = PropertiesLoader.getProperties();
        Properties appProps = new Properties();

        consumerPropsSet.forEach(prop -> {
            putProperty(appProps, fileProps, prop);
        });

        return appProps;
    }

    public static Properties getProducerKafkaProperties() {
        Properties fileProps = PropertiesLoader.getProperties();
        Properties appProps = new Properties();

        producerPropsSet.forEach(prop -> {
            putProperty(appProps, fileProps, prop);
        });

        return appProps;
    }

    public static String getProperty(String key) {
        if (key.isEmpty()) {
            return null;
        }

        Properties fileProps = PropertiesLoader.getProperties();
        return fileProps.getProperty(key);
    }

    private static void putProperty(Properties props, Properties fileProps, String property) {
        String propValue = fileProps.getProperty(property);
        log.info("init prop {} : {}", property, propValue);
        props.put(property, propValue);
    }
}
