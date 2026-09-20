package com.financialdata.streaming.binance.dto;

import java.math.BigDecimal;

public record BinancePriceResponse(String symbol, BigDecimal price) {
}
