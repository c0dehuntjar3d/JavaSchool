package sbp.school.kafka;

import org.junit.jupiter.api.Test;
import sbp.school.kafka.model.Transaction;
import sbp.school.kafka.model.Transaction.TransactionType;
import sbp.school.kafka.service.storage.InMemoryOutboxStorage;
import sbp.school.kafka.service.transaction.TransactionProducerService;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class TransactionProducerServiceTest {

    @Test
    public void testSendTransactionSuccess() {
        InMemoryOutboxStorage storage = new InMemoryOutboxStorage();
        TransactionProducerService producer = new TransactionProducerService("producer-1", storage);

        Transaction transaction = Transaction.builder()
                .id(UUID.randomUUID().toString())
                .account("test-account")
                .date(LocalDateTime.now())
                .value(BigDecimal.valueOf(100))
                .type(TransactionType.CREDIT)
                .build();

        assertDoesNotThrow(() -> producer.send(transaction));
        
        assertFalse(storage.isEmptySent());
        assertEquals(1, storage.getSentKeysFiltered(Long.MAX_VALUE).size());
    }

    @Test
    public void testResendTransaction() {
        InMemoryOutboxStorage storage = new InMemoryOutboxStorage();
        TransactionProducerService producer = new TransactionProducerService("producer-1", storage);

        Transaction transaction = Transaction.builder()
                .id(UUID.randomUUID().toString())
                .account("test-account")
                .date(LocalDateTime.now().minusMinutes(2)) 
                .value(BigDecimal.valueOf(100))
                .type(TransactionType.CREDIT)
                .build();

        producer.send(transaction);

        // Пытаемся повторно отправить транзакцию
        producer.run();

        assertTrue(storage.isEmptySent());
    }
}
