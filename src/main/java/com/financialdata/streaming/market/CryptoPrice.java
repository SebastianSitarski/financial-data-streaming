package com.financialdata.streaming.market;

import java.math.BigDecimal;

public record CryptoPrice(String symbol, BigDecimal price) {
}
