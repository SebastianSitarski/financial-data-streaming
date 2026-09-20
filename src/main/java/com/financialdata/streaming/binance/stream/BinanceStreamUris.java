package com.financialdata.streaming.binance.stream;

import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * The only place that knows how Binance names streams ({@code btcusdt@ticker}) and how a combined
 * stream URL is assembled from them.
 */
final class BinanceStreamUris {

    private static final String TICKER_SUFFIX = "@ticker";

    private BinanceStreamUris() {
    }

    static String tickerStream(String symbol) {
        return symbol.toLowerCase(Locale.ROOT) + TICKER_SUFFIX;
    }

    static URI combinedTickerStream(String wsUrl, List<String> symbols) {
        String streams = symbols.stream().map(BinanceStreamUris::tickerStream).collect(Collectors.joining("/"));
        String base = wsUrl.endsWith("/") ? wsUrl.substring(0, wsUrl.length() - 1) : wsUrl;
        return URI.create(base + "/stream?streams=" + streams);
    }
}
