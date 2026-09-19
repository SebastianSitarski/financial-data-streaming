package com.financialdata.streaming.binance;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import com.financialdata.streaming.market.Candle;
import org.springframework.stereotype.Component;

/**
 * Converts a raw Binance kline (a positional JSON array) into an application {@link Candle}.
 * This is the only place that knows the meaning of the array indexes. Any deviation from the
 * documented layout is reported as {@link IllegalArgumentException}.
 */
@Component
public class BinanceKlineMapper {

    private static final int OPEN_TIME = 0;
    private static final int OPEN = 1;
    private static final int HIGH = 2;
    private static final int LOW = 3;
    private static final int CLOSE = 4;
    private static final int VOLUME = 5;
    private static final int REQUIRED_LENGTH = VOLUME + 1;

    public Candle toCandle(List<Object> rawKline) {
        if (rawKline == null || rawKline.size() < REQUIRED_LENGTH) {
            throw new IllegalArgumentException(
                    "Kline must contain at least " + REQUIRED_LENGTH + " elements: " + rawKline);
        }
        return new Candle(
                epochMillis(rawKline.get(OPEN_TIME)),
                decimal(rawKline.get(OPEN)),
                decimal(rawKline.get(HIGH)),
                decimal(rawKline.get(LOW)),
                decimal(rawKline.get(CLOSE)),
                decimal(rawKline.get(VOLUME)));
    }

    public List<Candle> toCandles(List<List<Object>> rawKlines) {
        if (rawKlines == null) {
            throw new IllegalArgumentException("Klines payload is missing");
        }
        return rawKlines.stream().map(this::toCandle).toList();
    }

    private static Instant epochMillis(Object value) {
        if (!(value instanceof Number number)) {
            throw new IllegalArgumentException("Expected epoch millis but got: " + value);
        }
        return Instant.ofEpochMilli(number.longValue());
    }

    private static BigDecimal decimal(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("Kline contains a null numeric field");
        }
        // NumberFormatException is an IllegalArgumentException, so garbage strings surface the same way
        return new BigDecimal(value.toString());
    }
}
