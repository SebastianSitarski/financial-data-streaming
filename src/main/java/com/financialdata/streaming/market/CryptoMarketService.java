package com.financialdata.streaming.market;

import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.springframework.stereotype.Service;

@Service
public class CryptoMarketService {

    private static final Pattern SYMBOL_PATTERN = Pattern.compile("[A-Z0-9]{2,20}");
    private static final int MAX_ECHOED_SYMBOL_LENGTH = 32;

    private final MarketDataProvider marketData;

    public CryptoMarketService(MarketDataProvider marketData) {
        this.marketData = marketData;
    }

    public CryptoPrice getPrice(String symbol) {
        return marketData.getPrice(normalize(symbol));
    }

    public CryptoMarket getMarket(String symbol) {
        String normalized = normalize(symbol);
        SymbolInfo info = marketData.getSymbolInfo(normalized);
        CryptoPrice price = marketData.getPrice(normalized);
        return new CryptoMarket(info.symbol(), info.baseAsset(), info.quoteAsset(), info.status(), price.price());
    }

    public CryptoMarketStats getStats(String symbol) {
        return marketData.getStats(normalize(symbol));
    }

    public List<Candle> getCandles(String symbol, CandleInterval interval, int limit) {
        return marketData.getCandles(normalize(symbol), interval, limit);
    }

    static String normalize(String symbol) {
        String normalized = symbol == null ? "" : symbol.strip().toUpperCase(Locale.ROOT);
        if (!SYMBOL_PATTERN.matcher(normalized).matches()) {
            throw new InvalidRequestException("Invalid symbol '" + abbreviate(symbol) + "'");
        }
        return normalized;
    }

    private static String abbreviate(String symbol) {
        if (symbol == null || symbol.length() <= MAX_ECHOED_SYMBOL_LENGTH) {
            return symbol;
        }
        return symbol.substring(0, MAX_ECHOED_SYMBOL_LENGTH) + "...";
    }
}
