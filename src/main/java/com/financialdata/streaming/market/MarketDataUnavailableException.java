package com.financialdata.streaming.market;

public class MarketDataUnavailableException extends RuntimeException {

    public MarketDataUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
