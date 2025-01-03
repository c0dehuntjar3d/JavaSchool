package sbp.school.kafka.properties.loader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PropertiesLoader {

    public static Properties loadProperties(String propertiesFile) throws IOException {
        try (InputStream inputStream = PropertiesLoader.class.getClassLoader().getResourceAsStream(propertiesFile)) {
            Properties configuration = new Properties();
            configuration.load(inputStream);
            
            return configuration;
        } catch (Exception e) {
            log.error("error while reading file {}", propertiesFile, e);
            throw new IOException(e);
        }
    }
    
}
