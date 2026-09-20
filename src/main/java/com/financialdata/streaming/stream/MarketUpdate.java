package com.financialdata.streaming.stream;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Application-owned live market event, published to Kafka keyed by {@link #symbol()}.
 * {@code eventTime} is the provider's event timestamp, kept for latency and ordering analysis.
 */
public record MarketUpdate(
        String symbol,
        Instant eventTime,
        BigDecimal lastPrice,
        BigDecimal priceChange,
        BigDecimal priceChangePercent,
        BigDecimal highPrice,
        BigDecimal lowPrice,
        BigDecimal volume) {
}
