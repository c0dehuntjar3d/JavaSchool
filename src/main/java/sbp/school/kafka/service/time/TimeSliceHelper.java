package sbp.school.kafka.service.time;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import lombok.experimental.UtilityClass;

@UtilityClass
public class TimeSliceHelper {

    public static long getTimeSlice(LocalDateTime time, Duration timeout) {
        long mills = time.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
        return mills / timeout.toMillis();
    }
}
