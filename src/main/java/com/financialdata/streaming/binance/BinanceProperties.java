package com.financialdata.streaming.binance;

import java.time.Duration;

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
        @NotNull @DefaultValue("10s") Duration readTimeout) {
}
