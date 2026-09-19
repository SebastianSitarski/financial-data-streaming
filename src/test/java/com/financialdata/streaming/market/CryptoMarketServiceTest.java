package com.financialdata.streaming.market;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CryptoMarketServiceTest {

    private static final BigDecimal PRICE = new BigDecimal("115420.53");

    @Mock
    private MarketDataProvider marketData;

    @InjectMocks
    private CryptoMarketService service;

    @Test
    void getPriceDelegatesWithNormalizedSymbol() {
        CryptoPrice price = new CryptoPrice("BTCUSDT", PRICE);
        when(marketData.getPrice("BTCUSDT")).thenReturn(price);

        assertThat(service.getPrice("btcusdt")).isSameAs(price);
    }

    @ParameterizedTest
    @ValueSource(strings = { "btcusdt", "BtcUsdt", " BTCUSDT ", "BTCUSDT" })
    void symbolIsNormalizedToUppercaseBeforeCallingProvider(String input) {
        when(marketData.getPrice("BTCUSDT")).thenReturn(new CryptoPrice("BTCUSDT", PRICE));

        assertThat(service.getPrice(input).symbol()).isEqualTo("BTCUSDT");
        verify(marketData).getPrice("BTCUSDT");
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "   ", "BTC-USDT", "BTC_USDT", "B", "AVERYVERYLONGSYMBOLNAME123" })
    void rejectsBlankOrMalformedSymbolsWithoutCallingProvider(String input) {
        assertThatExceptionOfType(InvalidRequestException.class).isThrownBy(() -> service.getPrice(input));
        verifyNoInteractions(marketData);
    }

    @Test
    void oversizedSymbolIsAbbreviatedInErrorMessage() {
        String huge = "X".repeat(500);

        assertThatExceptionOfType(InvalidRequestException.class)
                .isThrownBy(() -> service.getPrice(huge))
                .withMessage("Invalid symbol '" + "X".repeat(32) + "...'");
    }

    @Test
    void getMarketCombinesSymbolMetadataAndPrice() {
        when(marketData.getSymbolInfo("ETHUSDT")).thenReturn(new SymbolInfo("ETHUSDT", "ETH", "USDT", "TRADING"));
        when(marketData.getPrice("ETHUSDT")).thenReturn(new CryptoPrice("ETHUSDT", PRICE));

        CryptoMarket market = service.getMarket("ethusdt");

        assertThat(market).isEqualTo(new CryptoMarket("ETHUSDT", "ETH", "USDT", "TRADING", PRICE));
    }

    @Test
    void getStatsDelegatesWithNormalizedSymbol() {
        CryptoMarketStats stats = new CryptoMarketStats("BTCUSDT", PRICE, BigDecimal.ONE, BigDecimal.TWO,
                PRICE, PRICE, BigDecimal.TEN);
        when(marketData.getStats("BTCUSDT")).thenReturn(stats);

        assertThat(service.getStats("btcusdt")).isSameAs(stats);
    }

    @Test
    void getCandlesPassesIntervalAndLimitToProvider() {
        List<Candle> candles = List.of(new Candle(Instant.EPOCH, BigDecimal.ONE, BigDecimal.TWO,
                BigDecimal.ONE, BigDecimal.TWO, BigDecimal.TEN));
        when(marketData.getCandles("SOLUSDT", CandleInterval.FOUR_HOURS, 50)).thenReturn(candles);

        assertThat(service.getCandles("solusdt", CandleInterval.FOUR_HOURS, 50)).isSameAs(candles);
    }
}
