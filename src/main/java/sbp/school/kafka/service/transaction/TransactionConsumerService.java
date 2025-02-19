package sbp.school.kafka.service.transaction;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.config.transaction.KafkaConsumerProperties;
import sbp.school.kafka.config.transaction.KafkaTransactionProperties;
import sbp.school.kafka.model.Transaction;
import sbp.school.kafka.service.AsyncCallback;
import sbp.school.kafka.service.KafkaConsumerFactory;
import sbp.school.kafka.service.storage.InboxStorage;
import sbp.school.kafka.service.time.TimeSliceHelper;

@Slf4j
public class TransactionConsumerService {

    private final String topic;
    private final ExecutorService executorService;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final InboxStorage storage;
    private final long duration;
    private final Duration timeout;

    private final Integer commitMaxProccessed;

    public TransactionConsumerService(InboxStorage storage) {
        this.storage = storage;
        this.commitMaxProccessed = KafkaTransactionProperties.getCommitMaxProcessed();
        this.timeout = KafkaTransactionProperties.getAckTime();
        this.topic = KafkaTransactionProperties.getTopic();
        this.executorService = Executors.newFixedThreadPool(KafkaTransactionProperties.PARTITIONS.size());
        this.duration = KafkaConsumerProperties.getCommitTimeout();
    }

    public void start() {
        running.set(true);

        for (Integer partitionIndex : KafkaTransactionProperties.PARTITIONS.values()) {
            executorService.submit(
                    () -> consumePartition(new TopicPartition(topic, partitionIndex)));
        }
    }

    public void close() {
        running.set(false);
    }

    public void consumePartition(TopicPartition partition) {
        KafkaConsumer<String, Transaction> consumer = KafkaConsumerFactory.getConsumerForPartition(partition);
        consumer.assign(Collections.singletonList(partition));

        log.info("Started consumer for partition {}", partition.partition());

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        OffsetAndMetadata prevOffsetAndMetadata = null;
        AsyncCallback asyncCallback = new AsyncCallback(topic);

        try {
            while (running.get()) {
                long startTime = System.currentTimeMillis();
                int processedRecords = 0;
                ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, Transaction> record : records) {
                    long timeSliceId = TimeSliceHelper.getTimeSlice(record.value().getDate(), timeout);
                    storage.save(timeSliceId, record.value());

                    processRecord(record);
                    currentOffsets.put(partition, new OffsetAndMetadata(record.offset() + 1));
                    processedRecords++;
                }

                boolean needCommit = processedRecords % commitMaxProccessed == 0 ||
                        System.currentTimeMillis() - startTime > duration;

                if (needCommit && currentOffsets.get(partition) != null &&
                        !currentOffsets.get(partition).equals(prevOffsetAndMetadata)) {

                    consumer.commitAsync(currentOffsets, asyncCallback);
                    prevOffsetAndMetadata = currentOffsets.get(partition);

                    startTime = System.currentTimeMillis();
                    processedRecords = 0;
                }
            }
        } catch (Exception e) {
            log.error("Error processing partition {}. {}", partition.partition(), Thread.currentThread().getName(),
                    e);
            throw new RuntimeException(e);
        } finally {
            try {
                consumer.commitSync(currentOffsets);
            } catch (Exception e) {
                log.error("Final commit failed for partition {}: {}", partition.partition(), e.getMessage());
            } finally {
                consumer.close();
            }
        }
    }

    private void processRecord(ConsumerRecord<String, Transaction> record) {
        log.info("Partition: {} | Offset: {} | Key: {} | Value: {}",
                record.partition(), record.offset(), record.key(), record.value().getAccount());
    }

}
