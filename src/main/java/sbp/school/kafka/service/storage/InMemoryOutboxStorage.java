package sbp.school.kafka.service.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.model.Transaction;
import sbp.school.kafka.service.checksum.ChecksumHelper;

@Slf4j
@ToString
public class InMemoryOutboxStorage implements OutboxStorage {

    private final Map<Long, List<Transaction>> sent = new ConcurrentHashMap<>();
    private final Map<Long, String> checksum = new ConcurrentHashMap<>();
    private final Map<String, Transaction> inProgress = new ConcurrentHashMap<>();

    @Override
    public void saveSent(long timeSliceKey, Transaction tx) {
        sent.computeIfAbsent(timeSliceKey, l -> new ArrayList<>()).add(tx);

        List<String> txIds = sent.get(timeSliceKey).stream().map(Transaction::getId).toList();
        String checksumForTimeSlice = ChecksumHelper.calculateChecksum(new ArrayList<>(txIds));

        checksum.put(timeSliceKey, checksumForTimeSlice);
    }

    @Override
    public void saveInProgress(Transaction tx) {
        inProgress.put(tx.getId(), tx);
    }

    @Override
    public void deleteInProgress(String txId) {
        inProgress.remove(txId);
    }

    @Override
    public void deleteSent(long timeSliceKey) {
        sent.remove(timeSliceKey);
    }

    @Override
    public String getChecksum(long timeSliceKey) {
        return checksum.get(timeSliceKey);
    }

    public void putChecksum(long timeSliceKey, String checksum) {
        this.checksum.put(timeSliceKey, checksum);
    }

    @Override
    public boolean isEmptyInProgress() {
        return inProgress.isEmpty();
    }

    @Override
    public boolean isEmptySent() {
        return sent.isEmpty();
    }

    @Override
    public Set<Long> getSentKeysFiltered(long timeoutTimeSlice) {
        return sent.keySet()
            .stream()
            .filter(key -> key < timeoutTimeSlice)
            .collect(Collectors.toSet());
    }

    @Override
    public void clear(Long timeSlice) {
        log.info("clearing... {}", timeSlice);
        sent.remove(timeSlice);
        checksum.remove(timeSlice);
    }

    @Override
    public List<Transaction> getSent(long key) {
        return sent.get(key);
    }

    @Override
    public void clear() {
        sent.clear();
        checksum.clear();
        inProgress.clear();
    }

    @Override
    public Set<Long> getTimeSlices() {
        return sent.keySet();
    }
    
}
