package com.financialdata.streaming.market;

import java.util.Arrays;

/**
 * Kline intervals supported by Binance Spot ({@code GET /api/v3/klines}).
 */
public enum CandleInterval {

    ONE_SECOND("1s"),
    ONE_MINUTE("1m"),
    THREE_MINUTES("3m"),
    FIVE_MINUTES("5m"),
    FIFTEEN_MINUTES("15m"),
    THIRTY_MINUTES("30m"),
    ONE_HOUR("1h"),
    TWO_HOURS("2h"),
    FOUR_HOURS("4h"),
    SIX_HOURS("6h"),
    EIGHT_HOURS("8h"),
    TWELVE_HOURS("12h"),
    ONE_DAY("1d"),
    THREE_DAYS("3d"),
    ONE_WEEK("1w"),
    ONE_MONTH("1M");

    private final String code;

    CandleInterval(String code) {
        this.code = code;
    }

    public static CandleInterval fromCode(String code) {
        return Arrays.stream(values())
                .filter(interval -> interval.code.equals(code))
                .findFirst()
                .orElseThrow(() -> new InvalidRequestException("Unsupported interval '" + code + "'"));
    }

    public String code() {
        return code;
    }
}
