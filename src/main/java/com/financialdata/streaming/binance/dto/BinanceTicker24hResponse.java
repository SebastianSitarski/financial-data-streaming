package com.financialdata.streaming.binance.dto;

import java.math.BigDecimal;

public record BinanceTicker24hResponse(
        String symbol,
        BigDecimal lastPrice,
        BigDecimal priceChange,
        BigDecimal priceChangePercent,
        BigDecimal highPrice,
        BigDecimal lowPrice,
        BigDecimal volume) {
}
