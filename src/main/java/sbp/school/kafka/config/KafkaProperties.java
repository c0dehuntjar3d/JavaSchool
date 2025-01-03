package sbp.school.kafka.config;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.properties.loader.PropertiesLoader;

@Slf4j
public class KafkaProperties {
    public static final String APPLICATION_PROPERTIES_FILE = "application.properties";

    private static Properties producerProps;
    private static Properties consumerProps;

    public static Set<String> producerPropsSet = Set.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        ProducerConfig.PARTITIONER_CLASS_CONFIG,
        ProducerConfig.ACKS_CONFIG,
        ProducerConfig.COMPRESSION_TYPE_CONFIG
    );

    public static Set<String> consumerPropsSet = Set.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        ConsumerConfig.GROUP_ID_CONFIG,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
        ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG
    );
    
    public static Properties getConsumerKafkaProperties() {
        if (consumerProps != null) {
            return consumerProps;
        }

        try {
            Properties fileProps = PropertiesLoader.loadProperties(APPLICATION_PROPERTIES_FILE);
            Properties appProps = new Properties();

            consumerPropsSet.forEach(prop -> {
                putProperty(appProps, fileProps, prop);
            });
            consumerProps = appProps;

            return consumerProps;
        } catch (IOException e) {
            log.error("error while loading props for consumer", e);
            throw new RuntimeException(e);
        }
    }

    public static Properties getProducerKafkaProperties() {
        if (producerProps != null) {
            return producerProps;
        }

        try {
            Properties fileProps = PropertiesLoader.loadProperties(APPLICATION_PROPERTIES_FILE);
            Properties appProps = new Properties();
    
            producerPropsSet.forEach(prop -> {
                putProperty(appProps, fileProps, prop);
            });
            producerProps = appProps;

            return producerProps;
        } catch (IOException e) {
            log.error("error while loading props for producer", e);
            throw new RuntimeException(e);
        }
    }

    public static String getProperty(String key) {
        if (key.isEmpty()) {
            return null;
        }

        try {
            Properties fileProps = PropertiesLoader.loadProperties(APPLICATION_PROPERTIES_FILE);

            return fileProps.getProperty(key);
        } catch (IOException e) {
            log.error("error while loading props", e);
            throw new RuntimeException(e);
        } 
    }

    private static void putProperty(Properties props, Properties fileProps, String property) {
        String propValue = fileProps.getProperty(property);
        log.info("init prop {} : {}", property, propValue);
        props.put(property, propValue);
    } 
}
