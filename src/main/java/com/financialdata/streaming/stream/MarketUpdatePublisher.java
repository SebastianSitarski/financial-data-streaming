package com.financialdata.streaming.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes {@link MarketUpdate}s to the market-update topic keyed by symbol, so all updates of one
 * symbol land on one partition and keep their order. Failures — asynchronous, or thrown synchronously as
 * Spring's {@link KafkaException} (KafkaTemplate wraps the metadata timeout after {@code max.block.ms},
 * serialization errors and a closed producer that way) — are logged, not propagated: the caller is the
 * WebSocket receive thread, and an exception there makes the container close the Binance connection,
 * which would turn a Kafka outage into a stream outage.
 */
@Component
public class MarketUpdatePublisher {

    private static final Logger log = LoggerFactory.getLogger(MarketUpdatePublisher.class);

    private final KafkaTemplate<String, MarketUpdate> kafkaTemplate;
    private final String topic;

    public MarketUpdatePublisher(KafkaTemplate<String, MarketUpdate> kafkaTemplate, MarketDataProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = properties.topic().name();
    }

    public void publish(MarketUpdate update) {
        try {
            kafkaTemplate.send(topic, update.symbol(), update).whenComplete((result, failure) -> {
                if (failure != null) {
                    log.warn("Failed to publish market update for {}: {}", update.symbol(), failure.toString());
                }
                else {
                    log.debug("Published market update {} to {}", update, result.getRecordMetadata());
                }
            });
        }
        catch (KafkaException ex) {
            log.warn("Failed to publish market update for {}: {}", update.symbol(), ex.toString());
        }
    }
}
