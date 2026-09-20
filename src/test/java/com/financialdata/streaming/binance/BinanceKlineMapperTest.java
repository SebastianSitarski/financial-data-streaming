package com.financialdata.streaming.binance;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import com.financialdata.streaming.market.Candle;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class BinanceKlineMapperTest {

    private final BinanceKlineMapper mapper = new BinanceKlineMapper();

    @Test
    void mapsPositionalKlineToCandle() {
        List<Object> rawKline = List.of(
                1499040000000L, "0.01634790", "0.80000000", "0.01575800", "0.01577100", "148976.11427815",
                1499644799999L, "2434.19055334", 308, "1756.87402397", "28.46694368", "0");

        Candle candle = mapper.toCandle(rawKline);

        assertThat(candle).isEqualTo(new Candle(
                Instant.ofEpochMilli(1499040000000L),
                new BigDecimal("0.01634790"),
                new BigDecimal("0.80000000"),
                new BigDecimal("0.01575800"),
                new BigDecimal("0.01577100"),
                new BigDecimal("148976.11427815")));
    }

    @Test
    void mapsListOfKlinesPreservingOrder() {
        List<List<Object>> rawKlines = List.of(
                List.of(1000L, "1", "2", "0.5", "1.5", "10"),
                List.of(2000L, "1.5", "3", "1", "2.5", "20"));

        List<Candle> candles = mapper.toCandles(rawKlines);

        assertThat(candles).extracting(Candle::openTime)
                .containsExactly(Instant.ofEpochMilli(1000L), Instant.ofEpochMilli(2000L));
        assertThat(candles.get(1).close()).isEqualByComparingTo("2.5");
    }

    @Test
    void rejectsTooShortKline() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> mapper.toCandle(List.of(1000L, "1", "2")));
    }

    @Test
    void rejectsNullNumericField() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> mapper.toCandle(Arrays.asList(1000L, "1", null, "0.5", "1.5", "10")))
                .withMessageContaining("null");
    }

    @Test
    void rejectsNonNumericPrice() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> mapper.toCandle(List.of(1000L, "abc", "2", "0.5", "1.5", "10")));
    }

    @Test
    void rejectsOpenTimeThatIsNotANumber() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> mapper.toCandle(List.of("1000", "1", "2", "0.5", "1.5", "10")))
                .withMessageContaining("epoch millis");
    }

    @Test
    void rejectsMissingPayload() {
        assertThatIllegalArgumentException().isThrownBy(() -> mapper.toCandles(null));
    }
}
