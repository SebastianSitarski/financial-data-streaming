package com.financialdata.streaming.binance.dto;

/**
 * Error payload returned by Binance, e.g. {@code {"code":-1121,"msg":"Invalid symbol."}}.
 */
public record BinanceApiError(int code, String msg) {

    public static final int INVALID_SYMBOL = -1121;
}
