package sbp.school.kafka.ack;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.Future;

import static org.mockito.Mockito.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import sbp.school.kafka.model.Ack;
import sbp.school.kafka.service.ack.AckProducerService;
import sbp.school.kafka.service.storage.InboxStorage;

public class AckProducerServiceTest {

    @Mock
    private KafkaProducer<String, Ack> mockProducer;

    @Mock
    private InboxStorage mockStorage;

    @Mock
    private Future<RecordMetadata> mockFuture;

    @Mock
    private RecordMetadata mockRecordMetadata;

    private AckProducerService ackProducerService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        ackProducerService = new AckProducerService("testId", mockProducer, mockStorage);
    }

    @Test
    void testSendAckSuccess() throws Exception {
        when(mockProducer.send(any(ProducerRecord.class))).thenReturn(mockFuture);
        when(mockFuture.get()).thenReturn(mockRecordMetadata);
        when(mockRecordMetadata.topic()).thenReturn("ack-topic");
        when(mockRecordMetadata.partition()).thenReturn(0);
        when(mockRecordMetadata.offset()).thenReturn(1L);

        Ack ack = new Ack(1L, "testChecksum");
        ackProducerService.send(ack);

        verify(mockProducer).send(any(ProducerRecord.class));
        verify(mockStorage).confirm(1L);
        verify(mockStorage).clear(1L);
    }

    @Test
    void testSendAckFailure() throws Exception {
        when(mockProducer.send(any(ProducerRecord.class))).thenThrow(new RuntimeException("Send Failed"));

        Ack ack = new Ack(1L, "testChecksum");
        ackProducerService.send(ack);

        verify(mockProducer).send(any(ProducerRecord.class));
        verify(mockStorage, never()).confirm(anyLong());
        verify(mockStorage, never()).clear(anyLong());
    }
}