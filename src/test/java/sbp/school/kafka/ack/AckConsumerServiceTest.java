package sbp.school.kafka.ack;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import sbp.school.kafka.config.KafkaProperties;
import sbp.school.kafka.model.Ack;
import sbp.school.kafka.model.Transaction;
import sbp.school.kafka.service.storage.InMemoryOutboxStorage;
import sbp.school.kafka.service.transaction.TransactionProducerService;
import sbp.school.kafka.service.ack.AckConsumerService;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Properties;

public class AckConsumerServiceTest {

        @Test
        public void testAckProcessingSuccess() {
                InMemoryOutboxStorage storage = new InMemoryOutboxStorage();

                long timeSliceKey = 1L;

                storage.saveSent(timeSliceKey, Transaction.builder()
                                .id("transaction-id-1")
                                .account("test-account")
                                .date(LocalDateTime.now())
                                .value(BigDecimal.valueOf(100))
                                .type(Transaction.TransactionType.CREDIT)
                                .build());
                storage.putChecksum(timeSliceKey, "expected-checksum");

                Ack ack = new Ack(timeSliceKey, "expected-checksum");

                ConsumerRecord<String, Ack> record = new ConsumerRecord<>(
                                "transaction-topic-ack",
                                0,
                                0,
                                "key",
                                ack);
                record.headers().add(KafkaProperties.PRODUCER_ID_PARAM, "1".getBytes());

                Properties props = KafkaProperties.getConsumerKafkaProperties();

                AckConsumerService consumer = new AckConsumerService(
                                props,
                                storage,
                                new TransactionProducerService("id", storage));

                consumer.processRecord(record);

                assertNull(storage.getChecksum(timeSliceKey));
                assertTrue(storage.isEmptySent());
        }

        @Test
        public void testAckProcessingFailureDueToChecksumMatch() {
                InMemoryOutboxStorage storage = new InMemoryOutboxStorage();
                long timeSliceKey = 1L;

                storage.saveSent(timeSliceKey, Transaction.builder()
                                .id("transaction-id-1")
                                .account("test-account")
                                .date(LocalDateTime.now())
                                .value(BigDecimal.valueOf(100))
                                .type(Transaction.TransactionType.CREDIT)
                                .build());

                Ack ack = new Ack(timeSliceKey, "wrong-checksum");

                ConsumerRecord<String, Ack> record = new ConsumerRecord<>(
                                "transaction-topic-ack",
                                0,
                                0,
                                "key",
                                ack);
                record.headers().add(KafkaProperties.PRODUCER_ID_PARAM, "1".getBytes());

                Properties props = KafkaProperties.getConsumerKafkaProperties();
                AckConsumerService consumer = new AckConsumerService(
                                props,
                                storage,
                                new TransactionProducerService("id", storage));

                consumer.processRecord(record);

                assertNull(storage.getChecksum(timeSliceKey));
                assertFalse(storage.isEmptySent());
        }
}
