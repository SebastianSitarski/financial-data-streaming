package com.financialdata.streaming.stream;

import java.math.BigDecimal;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MarketUpdateListenerTest {

    @Test
    void consumedUpdateIsStoredAsLatestState() {
        LatestMarketDataStore store = new LatestMarketDataStore();
        MarketUpdateListener listener = new MarketUpdateListener(store);
        MarketUpdate update = new MarketUpdate("ETHUSDT", Instant.parse("2026-09-20T09:42:21Z"),
                new BigDecimal("2639.29"), BigDecimal.ONE, BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE,
                BigDecimal.TEN);

        listener.onMarketUpdate(update);

        assertThat(store.get("ETHUSDT")).contains(update);
    }
}
