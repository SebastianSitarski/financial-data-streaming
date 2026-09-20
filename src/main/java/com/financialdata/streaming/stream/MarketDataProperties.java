package com.financialdata.streaming.stream;

import java.time.Duration;
import java.util.List;
import java.util.Locale;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * Live market-data pipeline settings: which symbols are streamed and where updates flow through Kafka.
 * Symbols are normalized to the application form ({@code BTCUSDT}) at binding time and must match the
 * same shape the REST API accepts, so a typo fails startup instead of taking the whole stream down.
 */
@Validated
@ConfigurationProperties(prefix = "market-data")
public record MarketDataProperties(
        @NotEmpty List<@NotBlank @Pattern(regexp = SYMBOL_PATTERN) String> symbols,
        @NotBlank @DefaultValue("market-update-processor") String consumerGroup,
        @Valid @DefaultValue Topic topic) {

    /** Same rule as {@code CryptoMarketService}: Binance symbols are plain alphanumerics. */
    static final String SYMBOL_PATTERN = "[A-Za-z0-9]{2,20}";

    public MarketDataProperties {
        symbols = symbols == null ? List.of()
                : symbols.stream().map(symbol -> symbol.strip().toUpperCase(Locale.ROOT)).distinct().toList();
    }

    /**
     * {@code retention} is deliberately short: each ticker is superseded within a second, so retained
     * history has no value for latest-state consumers and only lengthens a cold start.
     */
    public record Topic(
            @NotBlank @DefaultValue("crypto.market-updates") String name,
            @Min(1) @DefaultValue("3") int partitions,
            @Min(1) @DefaultValue("1") short replicationFactor,
            @NotNull @DefaultValue("1h") Duration retention) {
    }
}
