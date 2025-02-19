package sbp.school.kafka.service.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.model.Transaction;
import sbp.school.kafka.service.checksum.ChecksumHelper;

@Slf4j
@ToString
public class InMemoryInboxStorage implements InboxStorage {

    private final Map<Long, List<Transaction>> recieved = new ConcurrentHashMap<>();
    private final Map<Long, List<Transaction>> confirmed = new ConcurrentHashMap<>();
    private final Map<Long, String> checksum = new ConcurrentHashMap<>();

    @Override
    public void save(long timeSliceKey, Transaction tx) {
        recieved.computeIfAbsent(timeSliceKey, l -> new ArrayList<>()).add(tx);

        List<String> txIds = recieved.get(timeSliceKey).stream().map(Transaction::getId).toList();

        Optional<String> checksumForTimeSlice = ChecksumHelper.calculateChecksum(new ArrayList<>(txIds));
        if (checksumForTimeSlice.isEmpty()) {
            log.error("error while calculating checksum for timeSliceKey: {}, txIds: {}", timeSliceKey, txIds);
            return;
        }

        checksum.put(timeSliceKey, checksumForTimeSlice.get());
    }

    @Override
    public List<Transaction> getRecieved(Long timeSliceId) {
        return recieved.get(timeSliceId);
    }

    @Override
    public void clear(Long timeSliceId) {
        recieved.remove(timeSliceId);
        checksum.remove(timeSliceId);
    }

    @Override
    public boolean contains(Transaction transaction) {
        throw new UnsupportedOperationException("Unimplemented method 'contains'");
    }

    @Override
    public void clear() {
        recieved.clear();
        checksum.clear();
    }

    @Override
    public String getChecksum(long timeSlice) {
        return checksum.get(timeSlice);
    }

    @Override
    public Set<Long> getTimeSlices() {
        return recieved.keySet();
    }

    @Override
    public void confirm(Long timeSliceId) {
        List<Transaction> txs = recieved.get(timeSliceId);
        confirmed.computeIfAbsent(timeSliceId, l -> new ArrayList<>()).addAll(txs);
    }

    @Override
    public boolean isEmptyRecieved() {
        return recieved.isEmpty();
    }

}
