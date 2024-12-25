package sbp.school.kafka.config.transaction;

import sbp.school.kafka.properties.loader.PropertiesLoader;

public class KafkaConsumerProperties {
    private static final String CONSUMER_COMMIT_TIMEOUT_PROPERTY = "consumer.commit.timeout";

    public static Long getCommitTimeout() {
        return Long.parseLong(PropertiesLoader.getProperty(CONSUMER_COMMIT_TIMEOUT_PROPERTY));
    }

}
