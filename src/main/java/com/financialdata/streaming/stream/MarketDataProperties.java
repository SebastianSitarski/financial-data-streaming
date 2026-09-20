package com.financialdata.streaming.stream;

import java.util.List;
import java.util.Locale;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * Live market-data pipeline settings: which symbols are streamed and where updates flow through Kafka.
 * Symbols are normalized to the application form ({@code BTCUSDT}) at binding time.
 */
@Validated
@ConfigurationProperties(prefix = "market-data")
public record MarketDataProperties(
        @NotEmpty List<@NotBlank String> symbols,
        @NotBlank @DefaultValue("market-update-processor") String consumerGroup,
        @Valid @DefaultValue Topic topic) {

    public MarketDataProperties {
        symbols = symbols == null ? List.of()
                : symbols.stream().map(symbol -> symbol.strip().toUpperCase(Locale.ROOT)).distinct().toList();
    }

    public record Topic(
            @NotBlank @DefaultValue("crypto.market-updates") String name,
            @Min(1) @DefaultValue("3") int partitions,
            @Min(1) @DefaultValue("1") short replicationFactor) {
    }
}
