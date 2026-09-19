package com.financialdata.streaming.market;

import java.math.BigDecimal;

public record CryptoMarket(String symbol, String baseAsset, String quoteAsset, String status, BigDecimal price) {
}
