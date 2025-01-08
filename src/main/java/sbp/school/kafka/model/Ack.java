package sbp.school.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Ack {
    private final Long timeSliceId;
    private final String checksum;
}
