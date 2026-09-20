package com.financialdata.streaming.market;

public class CryptoSymbolNotFoundException extends RuntimeException {

    private final String symbol;

    public CryptoSymbolNotFoundException(String symbol) {
        super("Symbol " + symbol + " was not found");
        this.symbol = symbol;
    }

    public String getSymbol() {
        return symbol;
    }
}
