package com.financialdata.streaming.binance.dto;

public record BinanceSymbolInfo(String symbol, String status, String baseAsset, String quoteAsset) {
}
