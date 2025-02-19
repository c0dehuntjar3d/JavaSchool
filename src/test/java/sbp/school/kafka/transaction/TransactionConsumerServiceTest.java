package sbp.school.kafka.transaction;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import sbp.school.kafka.model.Transaction;
import sbp.school.kafka.service.storage.InMemoryOutboxStorage;
import sbp.school.kafka.service.storage.InboxStorage;
import sbp.school.kafka.service.transaction.TransactionConsumerService;
import sbp.school.kafka.service.transaction.TransactionProducerService;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class TransactionConsumerServiceTest {

    private TransactionConsumerService consumer;
    private InboxStorage inbox;

    @BeforeEach
    void setup() {
        inbox = mock(InboxStorage.class);
        consumer = new TransactionConsumerService(inbox);
    }

    @Test
    void testTransactionIsStored() {
        Transaction transaction = mock(Transaction.class);
        long timeSliceId = 12345L;

        inbox.save(timeSliceId, transaction);

        ArgumentCaptor<Long> timeSliceCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Transaction> transactionCaptor = ArgumentCaptor.forClass(Transaction.class);
        verify(inbox, times(1)).save(timeSliceCaptor.capture(), transactionCaptor.capture());

        assertEquals(timeSliceId, timeSliceCaptor.getValue());
        assertEquals(transaction, transactionCaptor.getValue());
    }

    @Test
    void testConsumerProcessesTransaction() throws InterruptedException {

        InMemoryOutboxStorage storage = new InMemoryOutboxStorage();
        TransactionProducerService producer = new TransactionProducerService("producer-1", storage);

        Transaction transaction = Transaction.builder()
                .id(UUID.randomUUID().toString())
                .account("test-account")
                .date(LocalDateTime.now())
                .value(BigDecimal.valueOf(100))
                .type(Transaction.TransactionType.CREDIT)
                .build();

        assertDoesNotThrow(() -> producer.send(transaction));

        consumer.start();
        Thread.sleep(5000);

        verify(inbox, atLeastOnce()).save(anyLong(), any(Transaction.class));
    }
}
