package sbp.school.kafka.service;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import sbp.school.kafka.config.KafkaProperties;
import sbp.school.kafka.model.Transaction;

public class KafkaConsumerFactory {

    public static KafkaConsumer<String, Transaction> getConsumerForPartition(TopicPartition partition) {
        Properties properties = KafkaProperties.getConsumerKafkaProperties();
        KafkaConsumer<String, Transaction> partitionConsumer = new KafkaConsumer<>(properties);
        partitionConsumer.assign(Collections.singleton(partition));
        return partitionConsumer;
    }

}
