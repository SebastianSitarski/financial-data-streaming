package com.financialdata.streaming.binance.stream.dto;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Binance {@code <symbol>@ticker} payload (24hr rolling window statistics), reduced to the fields this
 * application uses. Binance sends numbers as JSON strings; Jackson coerces them into {@link BigDecimal}
 * without going through a double.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BinanceTickerEvent(
        @JsonProperty("e") String eventType,
        @JsonProperty("E") Long eventTime,
        @JsonProperty("s") String symbol,
        @JsonProperty("c") BigDecimal lastPrice,
        @JsonProperty("p") BigDecimal priceChange,
        @JsonProperty("P") BigDecimal priceChangePercent,
        @JsonProperty("h") BigDecimal highPrice,
        @JsonProperty("l") BigDecimal lowPrice,
        @JsonProperty("v") BigDecimal volume) {
}
