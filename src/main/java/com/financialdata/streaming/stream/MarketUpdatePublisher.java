package com.financialdata.streaming.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes {@link MarketUpdate}s to the market-update topic keyed by symbol, so all updates of one
 * symbol land on one partition and keep their order. Sends are asynchronous; failures are logged, not
 * propagated, because the caller (the WebSocket receive thread) can do nothing useful with them.
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
        kafkaTemplate.send(topic, update.symbol(), update).whenComplete((result, failure) -> {
            if (failure != null) {
                log.warn("Failed to publish market update for {}: {}", update.symbol(), failure.toString());
            }
            else {
                log.debug("Published market update {} to {}", update, result.getRecordMetadata());
            }
        });
    }
}
