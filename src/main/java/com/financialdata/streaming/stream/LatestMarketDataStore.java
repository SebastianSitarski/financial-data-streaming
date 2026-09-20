package com.financialdata.streaming.stream;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

/**
 * In-memory latest state per symbol, fed by the Kafka consumer.
 *
 * <p>Semantics: an update replaces the stored one unless its {@code eventTime} is older. With a stable
 * symbol key updates arrive in partition order, so this guard normally never fires; it protects the
 * latest-state invariant against at-least-once redelivery interleaving after a consumer restart or
 * rebalance. Equal event times overwrite (a duplicate is identical, so the result is the same).
 */
@Component
public class LatestMarketDataStore {

    private final Map<String, MarketUpdate> latest = new ConcurrentHashMap<>();

    public void update(MarketUpdate update) {
        latest.merge(update.symbol(), update,
                (current, incoming) -> incoming.eventTime().isBefore(current.eventTime()) ? current : incoming);
    }

    public Optional<MarketUpdate> get(String symbol) {
        return Optional.ofNullable(latest.get(symbol));
    }
}
