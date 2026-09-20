package com.financialdata.streaming.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes market updates and keeps {@link LatestMarketDataStore} current. Processing is a map write,
 * so the listener never blocks the poll loop; delivery is at-least-once (offsets committed after the
 * batch is processed) and the store tolerates redelivery.
 *
 * <p>A group without committed offsets starts from the <em>latest</em> record: every ticker is
 * superseded a second later, so replaying retained history would only serve stale state as "live"
 * until the consumer catches up.
 */
@Component
public class MarketUpdateListener {

    private static final Logger log = LoggerFactory.getLogger(MarketUpdateListener.class);

    private final LatestMarketDataStore store;

    public MarketUpdateListener(LatestMarketDataStore store) {
        this.store = store;
    }

    @KafkaListener(topics = "${market-data.topic.name}", groupId = "${market-data.consumer-group}",
            properties = "auto.offset.reset=latest")
    public void onMarketUpdate(MarketUpdate update) {
        log.debug("Consumed market update {}", update);
        store.update(update);
    }
}
