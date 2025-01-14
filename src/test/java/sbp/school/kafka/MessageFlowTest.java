package sbp.school.kafka;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.config.KafkaProperties;
import sbp.school.kafka.model.Ack;
import sbp.school.kafka.model.Transaction;
import sbp.school.kafka.service.ack.AckConsumerService;
import sbp.school.kafka.service.ack.AckProducerService;
import sbp.school.kafka.service.storage.InMemoryInboxStorage;
import sbp.school.kafka.service.storage.InMemoryOutboxStorage;
import sbp.school.kafka.service.storage.InboxStorage;
import sbp.school.kafka.service.storage.OutboxStorage;
import sbp.school.kafka.service.transaction.TransactionConsumerService;
import sbp.school.kafka.service.transaction.TransactionProducerService;

@Slf4j
public class MessageFlowTest {

    private TransactionProducerService producer;
    private TransactionConsumerService consumer;

    private AckConsumerService ackConsumer;
    private AckProducerService ackProducer;

    private InboxStorage inbox;
    private OutboxStorage outbox;

    @BeforeEach
    public void setup() {
        inbox = new InMemoryInboxStorage();
        outbox = new InMemoryOutboxStorage();

        producer = new TransactionProducerService("id", outbox);
        consumer = new TransactionConsumerService(inbox);

        Properties cProps = KafkaProperties.getConsumerKafkaProperties();
        cProps.put(ConsumerConfig.GROUP_ID_CONFIG, "transactions-ack");
        cProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "sbp.school.kafka.serializer.ack.AckDeserializer");
        cProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        Properties pProps = KafkaProperties.getProducerKafkaProperties();
        pProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "sbp.school.kafka.serializer.ack.AckSerializer");
        pProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        pProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
                "sbp.school.kafka.partitioner.ack.AckPartitioner");

        ackConsumer = new AckConsumerService(cProps, outbox, producer);
        ackProducer = new AckProducerService(pProps, inbox, "id");

        ackConsumer.start();
        consumer.start();
    }

    @AfterEach
    private void close() {
        ackConsumer.close();
        consumer.close();
        producer.close();
        ackProducer.close();
    }

    @Test
    public void flowSuccessTest() throws InterruptedException {

        LocalDateTime baseTime = LocalDateTime.now().plusSeconds(new Random().nextInt(100));
        List<Transaction> txs = getTestTransactions(baseTime);
        txs.forEach(producer::send);

        Assertions.assertFalse(outbox.isEmptySent());
        Thread.sleep(Duration.ofMillis(500));

        Assertions.assertEquals(outbox.getTimeSlices(), inbox.getTimeSlices());

        outbox.getTimeSlices().forEach(timeSlice -> {
            Assertions.assertEquals(outbox.getSent(timeSlice).size(), inbox.getRecieved(timeSlice).size());
            Assertions.assertEquals(outbox.getChecksum(timeSlice), inbox.getChecksum(timeSlice));

            Ack ack = new Ack(timeSlice, inbox.getChecksum(timeSlice));
            ackProducer.send(ack);

            try {
                Thread.sleep(Duration.ofMillis(1000));
            } catch (InterruptedException e) {
            }
        });

        Assertions.assertTrue(outbox.isEmptySent());
    }

    @Test
    public void flowSuccessWithResend() throws InterruptedException {

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        Runnable runnable = producer;

        Duration retryDuration = Duration.ofSeconds(6);

        LocalDateTime baseTime = LocalDateTime.now();
        List<Transaction> txs = getTestTransactions(baseTime);
        txs.forEach(producer::send);

        scheduler.scheduleAtFixedRate(runnable, 0, retryDuration.toSeconds(), TimeUnit.SECONDS);
        Assertions.assertFalse(outbox.isEmptySent());

        log.info(outbox.toString());

        Thread.sleep(Duration.ofSeconds(7));

        outbox.getTimeSlices().forEach(timeSlice -> {
            Assertions.assertEquals(outbox.getSent(timeSlice).size(), inbox.getRecieved(timeSlice).size());
            Assertions.assertEquals(outbox.getChecksum(timeSlice), inbox.getChecksum(timeSlice));

            Ack ack = new Ack(timeSlice, inbox.getChecksum(timeSlice));
            ackProducer.send(ack);

            try {
                Thread.sleep(Duration.ofMillis(1000));
            } catch (InterruptedException e) {
            }
        });

        Assertions.assertTrue(outbox.isEmptySent());
        Assertions.assertFalse(inbox.isEmptyRecieved()); // остались полученные сообщния

        scheduler.shutdown();
    }

    private List<Transaction> getTestTransactions(LocalDateTime baseTime) {
        return List.of(
                Transaction.builder()
                        .id(UUID.randomUUID().toString())
                        .account("test-account")
                        .date(baseTime)
                        .value(BigDecimal.valueOf(100))
                        .type(Transaction.TransactionType.CREDIT)
                        .build(),
                Transaction.builder()
                        .id(UUID.randomUUID().toString())
                        .account("test-account5")
                        .date(baseTime.plusSeconds(3))
                        .value(BigDecimal.valueOf(200))
                        .type(Transaction.TransactionType.TRANSFER)
                        .build(),
                Transaction.builder()
                        .id(UUID.randomUUID().toString())
                        .account("test-account4")
                        .date(baseTime.plusSeconds(5))
                        .value(BigDecimal.valueOf(200))
                        .type(Transaction.TransactionType.CASH_TRANSITION)
                        .build());
    }

}
