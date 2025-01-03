package sbp.school.kafka.service.transaction;

import sbp.school.kafka.config.transaction.KafkaTransactionProperties;
import sbp.school.kafka.model.Transaction;
import sbp.school.kafka.service.KafkaProducerFactory;

import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransactionProducerService {

    private final KafkaProducer<String, Transaction> producer;
    private final String transactionTopic;

    public TransactionProducerService() {
        this.transactionTopic = KafkaTransactionProperties.getTopic();
        this.producer = KafkaProducerFactory.getProducer();
    }

    public void send(Transaction transaction) {
        try {
            Future<RecordMetadata> future = producer.send(
                    new ProducerRecord<>(
                        transactionTopic,
                        transaction.getType().name(),
                        transaction
                    )
                );

            RecordMetadata recordMetadata = future.get();
            log.debug(
                "topic: {}, partitions: {}, offset: {}", 
                recordMetadata.topic(),
                recordMetadata.partition(),
                recordMetadata.offset()
            );
        } catch (Exception e) {
            log.error("error sending message to {}: {}. {}", transactionTopic, transaction, e);
        }
    }

    public void close() {
        log.info("producer closed succesfully");
        producer.close();
    }
}
