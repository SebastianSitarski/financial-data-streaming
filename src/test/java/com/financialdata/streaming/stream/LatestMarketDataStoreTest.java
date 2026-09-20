package com.financialdata.streaming.stream;

import java.math.BigDecimal;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LatestMarketDataStoreTest {

    private static final Instant T0 = Instant.parse("2026-09-20T09:00:00Z");

    private final LatestMarketDataStore store = new LatestMarketDataStore();

    @Test
    void unknownSymbolIsEmpty() {
        assertThat(store.get("BTCUSDT")).isEmpty();
    }

    @Test
    void firstUpdateIsStored() {
        MarketUpdate first = update("BTCUSDT", T0, "100");

        store.update(first);

        assertThat(store.get("BTCUSDT")).contains(first);
    }

    @Test
    void newerUpdateReplacesOlder() {
        store.update(update("BTCUSDT", T0, "100"));
        MarketUpdate newer = update("BTCUSDT", T0.plusSeconds(1), "101");

        store.update(newer);

        assertThat(store.get("BTCUSDT")).contains(newer);
    }

    @Test
    void duplicateDeliveryLeavesLatestStateUnchanged() {
        MarketUpdate second = update("BTCUSDT", T0.plusSeconds(1), "101");
        MarketUpdate third = update("BTCUSDT", T0.plusSeconds(2), "102");

        store.update(update("BTCUSDT", T0, "100"));
        store.update(second);
        store.update(second);
        store.update(third);

        assertThat(store.get("BTCUSDT")).contains(third);
    }

    @Test
    void olderEventArrivingLateDoesNotOverwriteNewerState() {
        MarketUpdate newer = update("BTCUSDT", T0.plusSeconds(5), "105");

        store.update(newer);
        store.update(update("BTCUSDT", T0, "100"));

        assertThat(store.get("BTCUSDT")).contains(newer);
    }

    @Test
    void symbolsAreIndependent() {
        MarketUpdate btc = update("BTCUSDT", T0.plusSeconds(5), "105");
        MarketUpdate eth = update("ETHUSDT", T0, "10");

        store.update(btc);
        store.update(eth);

        assertThat(store.get("BTCUSDT")).contains(btc);
        assertThat(store.get("ETHUSDT")).contains(eth);
    }

    private static MarketUpdate update(String symbol, Instant eventTime, String lastPrice) {
        return new MarketUpdate(symbol, eventTime, new BigDecimal(lastPrice), BigDecimal.ONE, BigDecimal.ONE,
                BigDecimal.TEN, BigDecimal.ONE, BigDecimal.TEN);
    }
}
