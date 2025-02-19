package sbp.school.kafka.db;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class DatabaseSourceConnectorConfig extends AbstractConfig {
    public static final String HOSTNAME_CONFIG = "db.hostname";
    public static final String PORT_CONFIG = "db.port";
    public static final String DATABASE_CONFIG = "db.database";
    public static final String USERNAME_CONFIG = "db.username";
    public static final String PASSWORD_CONFIG = "db.password";
    public static final String TABLE_CONFIG = "db.table";
    public static final String TOPIC_CONFIG = "topic";
    public static final String OFFSET = "offset";
    public static final String LIMIT = "limit";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(HOSTNAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Database hostname")
            .define(PORT_CONFIG, ConfigDef.Type.INT, 5432, ConfigDef.Importance.HIGH, "Database port")
            .define(DATABASE_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Database name")
            .define(USERNAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Database username")
            .define(PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, "Database password")
            .define(TABLE_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Database table name")
            .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Kafka topic to publish data")
            .define(OFFSET, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "Database offset")
            .define(LIMIT, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "Database fetch limit");

    public DatabaseSourceConnectorConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
    }
}
