package com.financialdata.streaming.binance.stream;

import java.net.URI;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class BinanceStreamUrisTest {

    @ParameterizedTest
    @CsvSource({ "BTCUSDT, btcusdt@ticker", "ETHUSDT, ethusdt@ticker", "SOLUSDT, solusdt@ticker" })
    void tickerStreamIsLowercaseSymbolWithSuffix(String symbol, String expected) {
        assertThat(BinanceStreamUris.tickerStream(symbol)).isEqualTo(expected);
    }

    @Test
    void combinedStreamJoinsAllSymbolsUnderStreamPath() {
        URI uri = BinanceStreamUris.combinedTickerStream("wss://data-stream.binance.vision",
                List.of("BTCUSDT", "ETHUSDT", "SOLUSDT"));

        assertThat(uri).hasToString(
                "wss://data-stream.binance.vision/stream?streams=btcusdt@ticker/ethusdt@ticker/solusdt@ticker");
    }

    @Test
    void combinedStreamToleratesTrailingSlashInBaseUrl() {
        URI uri = BinanceStreamUris.combinedTickerStream("wss://data-stream.binance.vision/", List.of("BTCUSDT"));

        assertThat(uri).hasToString("wss://data-stream.binance.vision/stream?streams=btcusdt@ticker");
    }
}
