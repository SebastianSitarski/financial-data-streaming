package com.financialdata.streaming.market;

import java.time.Duration;
import java.util.Optional;

public class MarketDataRateLimitedException extends RuntimeException {

    private final Duration retryAfter;

    public MarketDataRateLimitedException(Duration retryAfter, Throwable cause) {
        super("Market data provider rate limit exceeded", cause);
        this.retryAfter = retryAfter;
    }

    /**
     * Back-off hint from the provider, when it sent one.
     */
    public Optional<Duration> getRetryAfter() {
        return Optional.ofNullable(retryAfter);
    }
}
