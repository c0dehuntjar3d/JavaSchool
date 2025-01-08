package sbp.school.kafka.service.transaction;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

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
import sbp.school.kafka.service.storage.OutboxStorage;

@Slf4j
public class TransactionConsumerService {

    private final String transactionTopic;
    private final ExecutorService executorService;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final long duration;
    private final Integer commitMaxProccessed;

    public TransactionConsumerService(OutboxStorage storage) {
        this.commitMaxProccessed = KafkaTransactionProperties.getCommitMaxProcessed();
        this.transactionTopic = KafkaTransactionProperties.getTopic();
        this.executorService = Executors.newFixedThreadPool(KafkaTransactionProperties.PARTITIONS.size());
        this.duration = KafkaConsumerProperties.getCommitTimeout();
    }

    public void start() {
        running.set(true);

        for (Integer partitionIndex : KafkaTransactionProperties.PARTITIONS.values()) {
            executorService.submit(
                    () -> consumePartition(new TopicPartition(transactionTopic, partitionIndex)));
        }
    }

    public void close() {
        running.set(false);
    }

    private void consumePartition(TopicPartition partition) {
        KafkaConsumer<String, Transaction> consumer = KafkaConsumerFactory.getConsumerForPartition(partition);
        consumer.assign(Collections.singletonList(partition));

        log.info("Started consumer for partition {}", partition.partition());

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        OffsetAndMetadata prevOffsetAndMetadata = null;
        AsyncCallback asyncCallback = new AsyncCallback(transactionTopic);

        try {
            while (running.get()) {
                long startTime = System.currentTimeMillis();
                int procceedRecords = 0;
                ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, Transaction> record : records) {
                    processRecord(record);
                    currentOffsets.put(partition, new OffsetAndMetadata(record.offset() + 1));
                    procceedRecords++;
                }

                if (procceedRecords % commitMaxProccessed == 0 || System.currentTimeMillis() - startTime > duration) {
                    boolean needCommit = currentOffsets.get(partition) != null
                            && !currentOffsets.get(partition).equals(prevOffsetAndMetadata);

                    if (needCommit) {
                        consumer.commitAsync(currentOffsets, asyncCallback);
                        prevOffsetAndMetadata = currentOffsets.get(partition);

                        startTime = System.currentTimeMillis();
                        procceedRecords = 0;
                    }


                }
            }
        } catch (Exception e) {
            log.error(
                    "Error processing partition {}. {} {}",
                    partition.partition(),
                    Thread.currentThread().getName(),
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
