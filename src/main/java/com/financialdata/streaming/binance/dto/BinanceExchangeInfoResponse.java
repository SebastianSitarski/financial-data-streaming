package com.financialdata.streaming.binance.dto;

import java.util.List;

public record BinanceExchangeInfoResponse(List<BinanceSymbolInfo> symbols) {
}
