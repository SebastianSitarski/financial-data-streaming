package com.financialdata.streaming.market;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.HttpHeaders;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(CryptoMarketController.class)
class CryptoMarketControllerTest {

    private static final BigDecimal PRICE = new BigDecimal("115420.53");

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private CryptoMarketService marketService;

    @Test
    void getPriceReturnsApplicationModel() throws Exception {
        when(marketService.getPrice("BTCUSDT")).thenReturn(new CryptoPrice("BTCUSDT", PRICE));

        mockMvc.perform(get("/api/crypto/BTCUSDT/price"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.symbol").value("BTCUSDT"))
                .andExpect(jsonPath("$.price").value(115420.53));
    }

    @Test
    void symbolPathVariableIsPassedToServiceUnchanged() throws Exception {
        when(marketService.getPrice("btcusdt")).thenReturn(new CryptoPrice("BTCUSDT", PRICE));

        mockMvc.perform(get("/api/crypto/btcusdt/price"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.symbol").value("BTCUSDT"));

        verify(marketService).getPrice("btcusdt");
    }

    @Test
    void getMarketReturnsCombinedModel() throws Exception {
        when(marketService.getMarket("BTCUSDT"))
                .thenReturn(new CryptoMarket("BTCUSDT", "BTC", "USDT", "TRADING", PRICE));

        mockMvc.perform(get("/api/crypto/BTCUSDT"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.baseAsset").value("BTC"))
                .andExpect(jsonPath("$.quoteAsset").value("USDT"))
                .andExpect(jsonPath("$.status").value("TRADING"))
                .andExpect(jsonPath("$.price").value(115420.53));
    }

    @Test
    void getStatsReturnsStatistics() throws Exception {
        when(marketService.getStats("BTCUSDT")).thenReturn(new CryptoMarketStats(
                "BTCUSDT", PRICE, new BigDecimal("2431.31"), new BigDecimal("2.15"),
                new BigDecimal("116200.00"), new BigDecimal("111900.00"), new BigDecimal("15234.42")));

        mockMvc.perform(get("/api/crypto/BTCUSDT/stats"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.lastPrice").value(115420.53))
                .andExpect(jsonPath("$.priceChangePercent").value(2.15))
                .andExpect(jsonPath("$.volume").value(15234.42));
    }

    @Test
    void unknownSymbolReturns404WithErrorBody() throws Exception {
        when(marketService.getPrice("BTCXYZ")).thenThrow(new CryptoSymbolNotFoundException("BTCXYZ"));

        mockMvc.perform(get("/api/crypto/BTCXYZ/price"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("CRYPTO_SYMBOL_NOT_FOUND"))
                .andExpect(jsonPath("$.message").value("Symbol BTCXYZ was not found"));
    }

    @Test
    void malformedSymbolReturns400() throws Exception {
        when(marketService.getPrice("INVALID_SYMBOL"))
                .thenThrow(new InvalidRequestException("Invalid symbol 'INVALID_SYMBOL'"));

        mockMvc.perform(get("/api/crypto/INVALID_SYMBOL/price"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
    }

    @Test
    void upstreamFailureReturns502WithoutLeakingDetails() throws Exception {
        when(marketService.getPrice("BTCUSDT"))
                .thenThrow(new MarketDataUnavailableException("Binance responded with HTTP 503", null));

        mockMvc.perform(get("/api/crypto/BTCUSDT/price"))
                .andExpect(status().isBadGateway())
                .andExpect(jsonPath("$.code").value("MARKET_DATA_UNAVAILABLE"))
                .andExpect(jsonPath("$.message").value("Market data provider is currently unavailable"));
    }

    @Test
    void rateLimitReturns429WithRetryAfterHeader() throws Exception {
        when(marketService.getPrice("BTCUSDT"))
                .thenThrow(new MarketDataRateLimitedException(Duration.ofSeconds(30), null));

        mockMvc.perform(get("/api/crypto/BTCUSDT/price"))
                .andExpect(status().isTooManyRequests())
                .andExpect(header().string(HttpHeaders.RETRY_AFTER, "30"))
                .andExpect(jsonPath("$.code").value("MARKET_DATA_RATE_LIMITED"));
    }

    @Test
    void rateLimitWithoutHintOmitsRetryAfterHeader() throws Exception {
        when(marketService.getPrice("BTCUSDT")).thenThrow(new MarketDataRateLimitedException(null, null));

        mockMvc.perform(get("/api/crypto/BTCUSDT/price"))
                .andExpect(status().isTooManyRequests())
                .andExpect(header().doesNotExist(HttpHeaders.RETRY_AFTER));
    }

    @Test
    void unexpectedExceptionReturns500InCommonFormat() throws Exception {
        when(marketService.getPrice("BTCUSDT")).thenThrow(new IllegalStateException("boom"));

        mockMvc.perform(get("/api/crypto/BTCUSDT/price"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.code").value("INTERNAL_ERROR"))
                .andExpect(jsonPath("$.message").value("Unexpected error"));
    }

    @Test
    void unsupportedMethodUsesCommonErrorFormat() throws Exception {
        mockMvc.perform(post("/api/crypto/BTCUSDT/price"))
                .andExpect(status().isMethodNotAllowed())
                .andExpect(jsonPath("$.code").value("METHOD_NOT_ALLOWED"));
    }

    @Test
    void unknownPathUsesCommonErrorFormat() throws Exception {
        mockMvc.perform(get("/api/nothing/here"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("NOT_FOUND"));
    }

    @Test
    void getCandlesPassesIntervalAndLimit() throws Exception {
        when(marketService.getCandles("BTCUSDT", CandleInterval.ONE_HOUR, 100)).thenReturn(List.of(new Candle(
                Instant.ofEpochMilli(1789812000000L), new BigDecimal("114500.00"), new BigDecimal("115100.00"),
                new BigDecimal("114300.00"), new BigDecimal("114950.00"), new BigDecimal("321.42"))));

        mockMvc.perform(get("/api/crypto/BTCUSDT/candles").param("interval", "1h").param("limit", "100"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].openTime").value("2026-09-19T10:00:00Z"))
                .andExpect(jsonPath("$[0].open").value(114500.00))
                .andExpect(jsonPath("$[0].close").value(114950.00))
                .andExpect(jsonPath("$[0].volume").value(321.42));
    }

    @Test
    void getCandlesUsesDefaultsWhenParametersAreOmitted() throws Exception {
        when(marketService.getCandles("BTCUSDT", CandleInterval.ONE_HOUR, 100)).thenReturn(List.of());

        mockMvc.perform(get("/api/crypto/BTCUSDT/candles"))
                .andExpect(status().isOk());

        verify(marketService).getCandles("BTCUSDT", CandleInterval.ONE_HOUR, 100);
    }

    @Test
    void getCandlesRejectsUnsupportedInterval() throws Exception {
        mockMvc.perform(get("/api/crypto/BTCUSDT/candles").param("interval", "7h"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"))
                .andExpect(jsonPath("$.message").value("Unsupported interval '7h'"));

        verify(marketService, never()).getCandles(anyString(), any(), anyInt());
    }

    @Test
    void getCandlesRejectsLimitAboveBinanceMaximumNamingTheParameter() throws Exception {
        mockMvc.perform(get("/api/crypto/BTCUSDT/candles").param("limit", "1001"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"))
                .andExpect(jsonPath("$.message").value(containsString("'limit'")))
                .andExpect(jsonPath("$.message").value(containsString("1000")));

        verify(marketService, never()).getCandles(anyString(), any(), anyInt());
    }

    @Test
    void getCandlesRejectsLimitBelowOne() throws Exception {
        mockMvc.perform(get("/api/crypto/BTCUSDT/candles").param("limit", "0"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"))
                .andExpect(jsonPath("$.message").value(containsString("'limit'")));

        verify(marketService, never()).getCandles(anyString(), any(), anyInt());
    }

    @Test
    void getCandlesRejectsNonNumericLimitNamingTheParameter() throws Exception {
        mockMvc.perform(get("/api/crypto/BTCUSDT/candles").param("limit", "many"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"))
                .andExpect(jsonPath("$.message").value("Parameter 'limit' has invalid value 'many'"));
    }
}
