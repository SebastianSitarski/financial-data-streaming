package com.financialdata.streaming.stream;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The one Kafka behavior unit tests cannot prove: the configured serializer, deserializer, type headers
 * and trusted packages actually round-trip a {@link MarketUpdate} from publisher to listener.
 */
@SpringBootTest(properties = "binance.stream.enabled=false")
@EmbeddedKafka(partitions = 1, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class MarketUpdateKafkaRoundTripTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    @Autowired
    private MarketUpdatePublisher publisher;

    @Autowired
    private LatestMarketDataStore store;

    @Test
    void publishedUpdateIsConsumedIntoTheLatestStateStore() throws InterruptedException {
        MarketUpdate update = new MarketUpdate("ROUNDTRIP", Instant.parse("2026-09-20T09:42:21.123Z"),
                new BigDecimal("116532.42000000"), new BigDecimal("1243.54"), new BigDecimal("1.080"),
                new BigDecimal("117100.00"), new BigDecimal("114220.50"), new BigDecimal("18452.43"));

        publisher.publish(update);

        assertThat(awaitStored("ROUNDTRIP")).contains(update);
    }

    private Optional<MarketUpdate> awaitStored(String symbol) throws InterruptedException {
        Instant deadline = Instant.now().plus(TIMEOUT);
        while (Instant.now().isBefore(deadline)) {
            Optional<MarketUpdate> stored = store.get(symbol);
            if (stored.isPresent()) {
                return stored;
            }
            Thread.sleep(100);
        }
        return store.get(symbol);
    }
}
