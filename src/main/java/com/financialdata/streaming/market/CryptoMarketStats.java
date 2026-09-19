package com.financialdata.streaming.market;

import java.math.BigDecimal;

public record CryptoMarketStats(
        String symbol,
        BigDecimal lastPrice,
        BigDecimal priceChange,
        BigDecimal priceChangePercent,
        BigDecimal highPrice,
        BigDecimal lowPrice,
        BigDecimal volume) {
}
