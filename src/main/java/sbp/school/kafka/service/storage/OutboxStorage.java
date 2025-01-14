package sbp.school.kafka.service.storage;

import java.util.List;
import java.util.Set;

import sbp.school.kafka.model.Transaction;

// Хранилище для отслеживания отправленных транзакций. 
public interface OutboxStorage {
    
    Set<Long> getSentKeysFiltered(long timeoutTimeSlice);
    Set<Long> getTimeSlices();
    List<Transaction> getSent(long key);
    void saveSent(long timeSliceKey, Transaction transaction);
    void deleteSent(long timeSliceKey);
    boolean isEmptySent();

    void saveInProgress(Transaction transaction);
    void deleteInProgress(String txId);
    boolean isEmptyInProgress();
    
    String getChecksum(long timeSliceKey);

    void clear(Long timeSlice);
    void clear();
}
