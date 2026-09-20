package com.financialdata.streaming.market;

import java.math.BigDecimal;
import java.time.Instant;

public record Candle(
        Instant openTime,
        BigDecimal open,
        BigDecimal high,
        BigDecimal low,
        BigDecimal close,
        BigDecimal volume) {
}
