package com.financialdata.streaming.stream;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MarketUpdatePublisherTest {

    private static final MarketUpdate UPDATE = new MarketUpdate("BTCUSDT", Instant.parse("2026-09-20T09:42:21.123Z"),
            new BigDecimal("116532.42"), new BigDecimal("1243.54"), new BigDecimal("1.08"),
            new BigDecimal("117100.00"), new BigDecimal("114220.50"), new BigDecimal("18452.43"));

    @Mock
    private KafkaTemplate<String, MarketUpdate> kafkaTemplate;

    private MarketUpdatePublisher publisher() {
        return new MarketUpdatePublisher(kafkaTemplate, new MarketDataProperties(List.of("BTCUSDT"),
                "market-update-processor", new MarketDataProperties.Topic("crypto.market-updates", 3, (short) 1)));
    }

    @Test
    void publishesToConfiguredTopicKeyedBySymbol() {
        when(kafkaTemplate.send("crypto.market-updates", "BTCUSDT", UPDATE))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher().publish(UPDATE);

        verify(kafkaTemplate).send("crypto.market-updates", "BTCUSDT", UPDATE);
    }

    @Test
    void sendFailureIsLoggedNotPropagated() {
        CompletableFuture<SendResult<String, MarketUpdate>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker down"));
        when(kafkaTemplate.send("crypto.market-updates", "BTCUSDT", UPDATE)).thenReturn(failed);

        assertThatNoException().isThrownBy(() -> publisher().publish(UPDATE));
    }
}
