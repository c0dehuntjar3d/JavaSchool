package sbp.school.kafka.service;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;

import sbp.school.kafka.config.KafkaProperties;
import sbp.school.kafka.model.Transaction;

public class CustomKafkaProducer {
    
    private static KafkaProducer<String, Transaction> producer;

    private CustomKafkaProducer() {}

    public static KafkaProducer<String, Transaction> getProducer() {
        if (producer == null) {
            Properties properties = KafkaProperties.getKafkaProducerProperties();
            producer = new KafkaProducer<>(properties);
            return producer;
        }
        
        return producer;
    }
}
