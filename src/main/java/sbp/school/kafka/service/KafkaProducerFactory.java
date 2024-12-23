package sbp.school.kafka.service;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;

import sbp.school.kafka.config.KafkaProperties;
import sbp.school.kafka.model.Transaction;

public class KafkaProducerFactory {
    
    public static KafkaProducer<String, Transaction> getProducer() {
        Properties properties = KafkaProperties.getKafkaProducerProperties();
        return new KafkaProducer<>(properties);
    }
}
