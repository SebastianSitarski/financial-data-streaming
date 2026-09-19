package com.financialdata.streaming.market;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class CandleIntervalTest {

    @ParameterizedTest
    @EnumSource(CandleInterval.class)
    void everyIntervalRoundTripsThroughItsCode(CandleInterval interval) {
        assertThat(CandleInterval.fromCode(interval.code())).isSameAs(interval);
    }

    @Test
    void codesAreCaseSensitiveBecauseBinanceDistinguishesMinuteFromMonth() {
        assertThat(CandleInterval.fromCode("1m")).isEqualTo(CandleInterval.ONE_MINUTE);
        assertThat(CandleInterval.fromCode("1M")).isEqualTo(CandleInterval.ONE_MONTH);
    }

    @Test
    void rejectsUnknownInterval() {
        assertThatExceptionOfType(InvalidRequestException.class)
                .isThrownBy(() -> CandleInterval.fromCode("7h"))
                .withMessageContaining("7h");
    }
}
