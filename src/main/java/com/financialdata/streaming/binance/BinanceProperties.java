package com.financialdata.streaming.binance;

import java.time.Duration;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "binance")
public record BinanceProperties(
        @NotBlank String baseUrl,
        @NotNull @DefaultValue("5s") Duration connectTimeout,
        @NotNull @DefaultValue("10s") Duration readTimeout,
        @NotBlank String wsUrl,
        @Valid @DefaultValue Stream stream) {

    /**
     * WebSocket market stream. Reconnects use exponential back-off from {@code initialBackoff} doubling
     * up to {@code maxBackoff}; the back-off resets once a connection has stayed up for {@code stableAfter},
     * so a flapping upstream cannot turn into a tight reconnect loop.
     */
    public record Stream(
            @DefaultValue("true") boolean enabled,
            @NotNull @DefaultValue("1s") Duration initialBackoff,
            @NotNull @DefaultValue("60s") Duration maxBackoff,
            @NotNull @DefaultValue("30s") Duration stableAfter) {
    }
}
