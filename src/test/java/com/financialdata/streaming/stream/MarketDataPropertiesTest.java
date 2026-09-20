package com.financialdata.streaming.stream;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class MarketDataPropertiesTest {

    private static final MarketDataProperties.Topic TOPIC =
            new MarketDataProperties.Topic("crypto.market-updates", 3, (short) 1, Duration.ofHours(1));

    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    void symbolsAreNormalizedAndDeduplicatedAtBinding() {
        MarketDataProperties properties = new MarketDataProperties(
                List.of(" btcusdt", "BtcUsdt", "ETHUSDT"), "market-update-processor", TOPIC);

        assertThat(properties.symbols()).containsExactly("BTCUSDT", "ETHUSDT");
        assertThat(validator.validate(properties)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = { "BTC/USDT", "BTC USDT", "BTC-USDT", "B", "AVERYVERYLONGSYMBOLNAME123" })
    void malformedSymbolFailsValidationInsteadOfReachingTheStream(String symbol) {
        MarketDataProperties properties = new MarketDataProperties(List.of(symbol), "market-update-processor", TOPIC);

        Set<ConstraintViolation<MarketDataProperties>> violations = validator.validate(properties);

        assertThat(violations).isNotEmpty();
        assertThat(violations).allSatisfy(v -> assertThat(v.getPropertyPath().toString()).startsWith("symbols"));
    }

    @Test
    void emptySymbolListIsRejected() {
        MarketDataProperties properties = new MarketDataProperties(List.of(), "market-update-processor", TOPIC);

        assertThat(validator.validate(properties)).isNotEmpty();
    }
}
