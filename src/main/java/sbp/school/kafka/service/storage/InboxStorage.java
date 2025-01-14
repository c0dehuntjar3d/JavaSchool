package sbp.school.kafka.service.storage;

import java.util.List;
import java.util.Set;

import sbp.school.kafka.model.Transaction;

public interface InboxStorage {
    void save(long timeSliceKey, Transaction transaction);

    Set<Long> getTimeSlices();

    List<Transaction> getRecieved(Long timeSliceId);

    void clear(Long timeSliceId);

    boolean contains(Transaction transaction);

    void clear();

    String getChecksum(long timeSliceId);

    void confirm(Long timeSliceId);

    boolean isEmptyRecieved();
}
