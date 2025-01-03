package sbp.school.kafka.partitioner.transaction;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.security.oauthbearer.secured.ValidateException;

import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.config.transaction.KafkaTransactionProperties;

@Slf4j
public class TransactionPartitioner implements Partitioner {

    private final static String PARTITION_DISMATCH_ERROR = "error partition amount dismatch";
    
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        validateKey(key, keyBytes);

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int size = partitions.size();

        if (size < KafkaTransactionProperties.PARTITIONS.size()) {
            log.error(PARTITION_DISMATCH_ERROR);
            throw new ValidateException(PARTITION_DISMATCH_ERROR);
        }

        return KafkaTransactionProperties.PARTITIONS.get(key);
    }

    private void validateKey(Object key, byte[] keyBytes) {
        if (keyBytes == null || !(key instanceof String)) {
            log.error("error while validating key: {}", key);
            throw new IllegalArgumentException("error while validating key:" + key.toString());
        }

        if (KafkaTransactionProperties.PARTITIONS.get(key) == null) {
            log.error("key missing: {}", key);
            throw new IllegalArgumentException("key missing:" + key);
        }
    }
   
    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public void close() {
        
    }

}
