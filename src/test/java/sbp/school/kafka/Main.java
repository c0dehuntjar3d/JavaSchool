package sbp.school.kafka;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.model.Transaction;
import sbp.school.kafka.model.Transaction.TransactionType;
import sbp.school.kafka.service.transaction.TransactionConsumerService;
import sbp.school.kafka.service.transaction.TransactionProducerService;

@Slf4j
public class Main {

    @Test
    public void testSendTransactionSuccess() {
        TransactionProducerService producerService = new TransactionProducerService();

        List<Transaction> transaction = getTestData();
        
        assertDoesNotThrow(() -> {
            transaction.forEach(producerService::send);
        });
        assertDoesNotThrow(() -> producerService.close());
    }

    @Test
    public void testReadTransactionSuccess() {
        TransactionConsumerService service = new TransactionConsumerService();
        assertDoesNotThrow(() -> service.start());
        
        TransactionProducerService producerService = new TransactionProducerService();
        List<Transaction> transaction = getTestData();
        transaction.forEach(producerService::send);
        
        assertDoesNotThrow(() -> service.close());
    } 

    public List<Transaction> getTestData() {
        return List.of(
            Transaction.builder()
                .id(1)
                .account("acc1")
                .date(LocalDateTime.now())
                .value(BigDecimal.valueOf(100))
                .type(TransactionType.CREDIT) 
                .build(),
            Transaction.builder()
                .id(2)
                .account("ac1")
                .date(LocalDateTime.now())
                .value(BigDecimal.valueOf(100))
                .type(TransactionType.CREDIT) 
                .build(),
            Transaction.builder()
                .id(3)
                .account("acc1")
                .date(LocalDateTime.now())
                .value(BigDecimal.valueOf(100))
                .type(TransactionType.TRANSFER) 
                .build(),
            Transaction.builder()
                .id(4)
                .account("acc1")
                .date(LocalDateTime.now())
                .value(BigDecimal.valueOf(100))
                .type(TransactionType.CASH_TRANSITION) 
                .build(),
            Transaction.builder()
                .id(5)
                .account("acc5")
                .date(LocalDateTime.now())
                .value(BigDecimal.valueOf(1000))
                .type(TransactionType.BROCKER_SERVICE) 
                .build()
        );
    }

}
