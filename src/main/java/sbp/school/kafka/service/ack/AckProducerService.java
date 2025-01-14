package sbp.school.kafka.service.ack;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.config.KafkaProperties;
import sbp.school.kafka.config.transaction.KafkaTransactionProperties;
import sbp.school.kafka.model.Ack;
import sbp.school.kafka.service.storage.InboxStorage;

@Slf4j
public class AckProducerService extends Thread {
    private final String topic;
    private final String id;

    private final KafkaProducer<String, Ack> producer;
    private final InboxStorage storage;

    public AckProducerService(Properties properties, InboxStorage storage, String id) {
        this.producer = new KafkaProducer<>(properties);
        this.id = id;
        this.topic = KafkaTransactionProperties.getAckTopic();
        this.storage = storage;
    }

    public void send(Ack ack) {
        try {
            ProducerRecord<String, Ack> record = new ProducerRecord<String, Ack>(topic, ack.getChecksum(), ack);
            record.headers().add(KafkaProperties.PRODUCER_ID_PARAM, id.getBytes());

            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata recordMetadata = future.get();
            log.info(
                    "message send success!\n topic: {}, partitions: {}, offset: {}",
                    recordMetadata.topic(),
                    recordMetadata.partition(),
                    recordMetadata.offset());

            storage.confirm(ack.getId());
            storage.clear(ack.getId());

        } catch (Exception e) {
            log.error("error sending message to {}: {}. {}", topic, ack, e.toString());
        }
    }

    public void close() {
        log.info("producer closed succesfully");
        producer.close();
    }

}
