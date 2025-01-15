package sbp.school.kafka.service.checksum;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@UtilityClass
public class ChecksumHelper {

    public static Optional<String> calculateChecksum(List<String> txIds) {
        if (txIds == null || txIds.isEmpty()) {
            log.error("txIds is null or empty {}", txIds);
            return Optional.empty();
        }

        Collections.sort(txIds);

        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            String combined = String.join(",", txIds);
            byte[] hash = digest.digest(combined.getBytes(StandardCharsets.UTF_8));
            return Optional.of(bytesToHex(hash));
        } catch (NoSuchAlgorithmException e) {
            log.error("Failed to calculate checksum", e);
            return Optional.empty();
        }
    }

    private static String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }
}
