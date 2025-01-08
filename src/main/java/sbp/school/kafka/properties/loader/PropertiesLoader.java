package sbp.school.kafka.properties.loader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.config.KafkaProperties;

@Slf4j
public class PropertiesLoader {

    private static final Properties properties = new Properties();

    static {
        try {
            loadProperties(KafkaProperties.APPLICATION_PROPERTIES_FILE);
        } catch (IOException e) {
            log.error("error while loading props: {}, {}", KafkaProperties.APPLICATION_PROPERTIES_FILE, e);
            throw new RuntimeException(e);
        }
    }

    private static void loadProperties(String propertiesFile) throws IOException {
        try (InputStream inputStream = PropertiesLoader.class.getClassLoader().getResourceAsStream(propertiesFile)) {
            properties.load(inputStream);
        } catch (Exception e) {
            log.error("error while reading file {}", propertiesFile, e);
            throw new IOException(e);
        }
    }

    public static Properties getProperties() {
        return properties;
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }
}
