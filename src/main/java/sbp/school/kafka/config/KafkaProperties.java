package sbp.school.kafka.config;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;

import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.properties.loader.PropertiesLoader;

@Slf4j
public class KafkaProperties {
    public static final String PRODUCER_PROPERTIES_FILE = "application.properties";

    public static List<String> props = List.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        ProducerConfig.PARTITIONER_CLASS_CONFIG,
        ProducerConfig.ACKS_CONFIG,
        ProducerConfig.COMPRESSION_TYPE_CONFIG
    );

    public static Properties getKafkaProducerProperties() {
        try {
            Properties fileProps = PropertiesLoader.loadProperties(PRODUCER_PROPERTIES_FILE);
            Properties appProps = new Properties();

            props.forEach(prop -> {
                putProperty(appProps, fileProps, prop);
            });

            return appProps;
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
