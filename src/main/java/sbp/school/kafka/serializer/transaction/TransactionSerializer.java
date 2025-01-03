package sbp.school.kafka.serializer.transaction;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.security.oauthbearer.secured.ValidateException;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.model.Transaction;

@Slf4j
public class TransactionSerializer implements Serializer<Transaction> {

    private static ObjectMapper transactionMapper;
    private static final String TRANSACTION_SCHEME_FILE_PATH = "src/main/resources/json/transaction.json"; 

    private static ObjectMapper getTransactionMapper() {
        if (transactionMapper == null) {
            transactionMapper = new ObjectMapper();
            transactionMapper.registerModule(new JavaTimeModule());
            transactionMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

            return transactionMapper;
        }

        return transactionMapper;
    }

    @Override
    public byte[] serialize(String topic, Transaction data) {
        if (data == null) {
            log.error("data is null for topic: {}", topic);
            return null;
        }

        ObjectMapper mapper = getTransactionMapper();
        
        try {
            JsonNode jsonSchema = mapper.readTree(new File(TRANSACTION_SCHEME_FILE_PATH));
            String value = mapper.writeValueAsString(data);
            JsonNode jsonValue = mapper.readTree(value);

            JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
            JsonSchema schema = factory.getSchema(jsonSchema);

            validate(schema.validate(jsonValue), value); 

            return value.getBytes(StandardCharsets.UTF_8);            
        } catch (SerializationException e) {
            log.error("serialize error for {}: {}", data, e.getMessage());
            throw new RuntimeException(e);
        } catch (IOException e) {
            log.error("file error: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
    
    private void validate(Set<ValidationMessage> validationMessages, String value) {
        if (!validationMessages.isEmpty()) {
            String validationExceptionMessages = validationMessages.stream()
                    .map(ValidationMessage::getMessage)
                    .collect(Collectors.joining(", "));

            log.error("validation error for: {}, {}", value, validationExceptionMessages);
            throw new ValidateException(validationExceptionMessages);
        }
    }

}
