package sbp.school.kafka.service.ack;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.config.KafkaProperties;
import sbp.school.kafka.config.transaction.KafkaTransactionProperties;
import sbp.school.kafka.model.Ack;
import sbp.school.kafka.service.AsyncCallback;
import sbp.school.kafka.service.storage.OutboxStorage;

@Slf4j
public class AckConsumerService extends Thread {

    private final String topic;
    private final OutboxStorage storage;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ExecutorService executorService;

    private final KafkaConsumer<String, Ack> consumer;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public AckConsumerService(Properties properties, OutboxStorage storage) {
        this.storage = storage;
        this.executorService = Executors.newFixedThreadPool(1);
        this.consumer = new KafkaConsumer<>(properties);
        this.topic = KafkaTransactionProperties.getAcktTopioc();
    }

    public void start() {
        running.set(true);
        executorService.submit(this::consume);
    }

    public void close() {
        running.set(false);
    }

    private void consume() {
        consumer.subscribe(Collections.singletonList(topic));

        try {
            while (running.get()) {
                ConsumerRecords<String, Ack> records = consumer.poll(Duration.ofMillis(100));

                records.forEach(record -> {
                    processRecord(record);

                    currentOffsets.put(
                            new TopicPartition(topic, record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));
                });

                if (!currentOffsets.isEmpty()) {
                    consumer.commitAsync(currentOffsets, new AsyncCallback(topic));
                }
            }

        } catch (Exception e) {
            log.error(
                    "Error processing {}. {}",
                    Thread.currentThread().getName(),
                    e);
            throw new RuntimeException(e);
        } finally {
            try {
                consumer.commitSync(currentOffsets);
            } catch (Exception e) {
                log.error("Final commit failed {}", e.getMessage());
            } finally {
                consumer.close();
            }
        }
    }

    public void processRecord(ConsumerRecord<String, Ack> record) {
        String producerId = new String(record.headers().lastHeader(KafkaProperties.PRODUCER_ID_PARAM).value());
        if (producerId.isBlank()) {
            log.warn("missing producer id for record: {}", record.topic());
            return;
        }

        Ack ack = record.value();

        long timeSliceId = ack.getTimeSliceId();
        String checksum = storage.getChecksum(timeSliceId);

        if (checksum == null) {
            log.warn("ack with no key recieved");
            return;
        }

        if (checksum.equals(ack.getChecksum())) {
            storage.clear(timeSliceId);
            log.info(
                    "ack recieved and processed success:\n ProducerId: {}, key: {}",
                    producerId,
                    ack.getTimeSliceId());
            return;
        } else {
            log.warn(
                    "ack recieved and processed failure: checksum dismatch: \n ProducerId: {}, key: {}, checksum: {}, actual checksum {}",
                    producerId,
                    timeSliceId,
                    ack.getChecksum(),
                    checksum);
        }
    }

}
