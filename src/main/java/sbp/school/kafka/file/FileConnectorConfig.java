package sbp.school.kafka.file;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class FileConnectorConfig extends AbstractConfig {

    public static final String TOPIC_CONFIG = "topics";
    public static final String FILE = "file";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Kafka topic to publish data")
            .define(FILE, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "File");

    public FileConnectorConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
    }
}
