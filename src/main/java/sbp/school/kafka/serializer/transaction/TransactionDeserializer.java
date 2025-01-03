package sbp.school.kafka.serializer.transaction;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.security.oauthbearer.secured.ValidateException;
import org.apache.kafka.common.serialization.Deserializer;
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
public class TransactionDeserializer implements Deserializer<Transaction> {

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
    public Transaction deserialize(String topic, byte[] data) {
        if (data == null) {
            log.error("data is null for topic: {}", topic);
            return null;
        }

        ObjectMapper mapper = getTransactionMapper();

        try {
            JsonNode jsonSchema = mapper.readTree(new File(TRANSACTION_SCHEME_FILE_PATH));
            JsonNode jsonValue = mapper.readTree(data);

            JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
            JsonSchema schema = factory.getSchema(jsonSchema);

            validate(schema.validate(jsonValue)); 

            return mapper.readValue(data, Transaction.class);
        } catch (IOException e) {
            System.out.println("JsonProcessingException: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void validate(Set<ValidationMessage> validationMessages) {
        if (!validationMessages.isEmpty()) {
            String validationExceptionMessages = validationMessages.stream()
                    .map(ValidationMessage::getMessage)
                    .collect(Collectors.joining(", "));

            log.error("validation error for: {}", validationExceptionMessages);
            throw new ValidateException(validationExceptionMessages);
        }
    }
   
}
