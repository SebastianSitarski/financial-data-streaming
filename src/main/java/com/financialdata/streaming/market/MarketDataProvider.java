package com.financialdata.streaming.market;

import java.util.List;

/**
 * Source of market data expressed purely in application models. Adapters for concrete
 * exchanges implement it and keep their transport DTOs to themselves.
 */
public interface MarketDataProvider {

    CryptoPrice getPrice(String symbol);

    SymbolInfo getSymbolInfo(String symbol);

    CryptoMarketStats getStats(String symbol);

    List<Candle> getCandles(String symbol, CandleInterval interval, int limit);
}
