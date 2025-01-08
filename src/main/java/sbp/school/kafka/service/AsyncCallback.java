package sbp.school.kafka.service;

import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class AsyncCallback implements OffsetCommitCallback {

    private final String topic;

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if (exception != null) {
            log.error(
                    "Failed commit topic: {} | offsets: {} | Error: {}",
                    topic,
                    offsets,
                    exception);
        } else {
            log.debug(
                    "Success commit topic: {} | offsets: {}",
                    topic,
                    offsets);
        }
    }
}
