package com.financialdata.streaming.market;

/**
 * No live update can be served for a symbol: either it is not part of the configured stream
 * subscriptions, or it is but nothing has flowed through the pipeline for it yet.
 */
public class LiveMarketDataNotAvailableException extends RuntimeException {

    private LiveMarketDataNotAvailableException(String message) {
        super(message);
    }

    public static LiveMarketDataNotAvailableException notSubscribed(String symbol) {
        return new LiveMarketDataNotAvailableException("Symbol " + symbol + " is not part of the live subscriptions");
    }

    public static LiveMarketDataNotAvailableException noUpdateYet(String symbol) {
        return new LiveMarketDataNotAvailableException("No live update has been received yet for " + symbol);
    }
}
