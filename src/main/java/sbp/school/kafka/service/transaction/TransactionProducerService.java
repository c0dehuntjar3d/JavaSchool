package sbp.school.kafka.service.transaction;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.config.KafkaProperties;
import sbp.school.kafka.config.transaction.KafkaProducerProperties;
import sbp.school.kafka.config.transaction.KafkaTransactionProperties;
import sbp.school.kafka.model.Transaction;
import sbp.school.kafka.service.KafkaProducerFactory;
import sbp.school.kafka.service.storage.OutboxStorage;
import sbp.school.kafka.service.time.TimeSliceHelper;

@Slf4j
public class TransactionProducerService extends Thread {


    private final String id;
    private final String transactionTopic;

    public final KafkaProducer<String, Transaction> producer;
    private final OutboxStorage storage;

    private final Duration timeout;

    public TransactionProducerService(String id, OutboxStorage storage) {
        this.id = id;
        this.storage = storage;
        this.timeout = KafkaProducerProperties.getAckTime();
        this.transactionTopic = KafkaTransactionProperties.getTopic();
        this.producer = KafkaProducerFactory.getProducer();
    }

    public void send(Transaction transaction) {
        long timeSlice = TimeSliceHelper.getTimeSlice(transaction.getDate(), timeout);

        try {

            ProducerRecord<String, Transaction> record = new ProducerRecord<>(
                    transactionTopic,
                    transaction.getType().name(),
                    transaction);
            record.headers().add(KafkaProperties.PRODUCER_ID_PARAM, id.getBytes());

            storage.saveInProgress(transaction);

            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata recordMetadata = future.get();

            storage.saveSent(timeSlice, transaction);

            log.info(
                    "message send success!\n topic: {}, partitions: {}, offset: {}",
                    recordMetadata.topic(),
                    recordMetadata.partition(),
                    recordMetadata.offset());

            storage.deleteInProgress(transaction.getId());

        } catch (Exception e) {
            log.error("error sending message to {}: {}. {}", transactionTopic, transaction, e.toString());
        }
    }

    @Override
    public void run() {
        resend();
    }

    private void resend() {
        if (storage.isEmptySent()) {
            log.info("nothing to resend");
            return;
        }

        LocalDateTime now = LocalDateTime.now();
        long newTimeSliceId = TimeSliceHelper.getTimeSlice(now.minus(timeout), timeout);

        Set<Long> timeSlicesToRetry = storage.getSentKeysFiltered(newTimeSliceId);

        timeSlicesToRetry.forEach(slice -> {
            resendTransaction(slice, now);
            storage.clear(slice);

            log.info("resend tx: {}", slice);
        });
    }

    private void resendTransaction(long timeSliceId, LocalDateTime time) {
        List<Transaction> transactions = storage.getSent(timeSliceId);

        transactions.forEach(t -> {
            Transaction newTransaction = Transaction.builder()
                    .id(t.getId())
                    .type(t.getType())
                    .account(t.getAccount())
                    .date(time)
                    .build();
            send(newTransaction);
        });
    }

    public void close() {
        log.info("producer closed succesfully");
        producer.close();
    }

    public String getProducerId() {
        return id;
    }

}
