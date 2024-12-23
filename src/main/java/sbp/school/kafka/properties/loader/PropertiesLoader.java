package sbp.school.kafka.properties.loader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PropertiesLoader {

    public static Properties loadProperties(String propertiesFile) throws IOException {
        Properties configuration = new Properties();
        InputStream inputStream = PropertiesLoader.class.getClassLoader().getResourceAsStream(propertiesFile);
        
        configuration.load(inputStream);
        inputStream.close();
        return configuration;
    }
    
}
