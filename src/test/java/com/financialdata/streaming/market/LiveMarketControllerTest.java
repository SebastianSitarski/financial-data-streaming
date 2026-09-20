package com.financialdata.streaming.market;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import com.financialdata.streaming.stream.LatestMarketDataStore;
import com.financialdata.streaming.stream.MarketDataProperties;
import com.financialdata.streaming.stream.MarketUpdate;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LiveMarketController.class)
@Import(LiveMarketControllerTest.SubscribedSymbols.class)
class LiveMarketControllerTest {

    private static final MarketUpdate BTC = new MarketUpdate("BTCUSDT", Instant.parse("2026-09-20T09:42:21.123Z"),
            new BigDecimal("116532.42"), new BigDecimal("1243.54"), new BigDecimal("1.08"),
            new BigDecimal("117100.00"), new BigDecimal("114220.50"), new BigDecimal("18452.43"));

    @TestConfiguration(proxyBeanMethods = false)
    static class SubscribedSymbols {
        @Bean
        MarketDataProperties marketDataProperties() {
            return new MarketDataProperties(List.of("BTCUSDT", "ETHUSDT"), "market-update-processor",
                    new MarketDataProperties.Topic("crypto.market-updates", 3, (short) 1));
        }
    }

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private LatestMarketDataStore store;

    @Test
    void returnsLatestConsumedUpdate() throws Exception {
        when(store.get("BTCUSDT")).thenReturn(Optional.of(BTC));

        mockMvc.perform(get("/api/crypto/BTCUSDT/live"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.symbol").value("BTCUSDT"))
                .andExpect(jsonPath("$.eventTime").value("2026-09-20T09:42:21.123Z"))
                .andExpect(jsonPath("$.lastPrice").value(116532.42))
                .andExpect(jsonPath("$.priceChange").value(1243.54))
                .andExpect(jsonPath("$.priceChangePercent").value(1.08))
                .andExpect(jsonPath("$.highPrice").value(117100.00))
                .andExpect(jsonPath("$.lowPrice").value(114220.50))
                .andExpect(jsonPath("$.volume").value(18452.43));
    }

    @Test
    void lowercaseSymbolIsNormalized() throws Exception {
        when(store.get("BTCUSDT")).thenReturn(Optional.of(BTC));

        mockMvc.perform(get("/api/crypto/btcusdt/live"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.symbol").value("BTCUSDT"));
    }

    @Test
    void subscribedSymbolWithoutUpdateYetReturns404() throws Exception {
        when(store.get("ETHUSDT")).thenReturn(Optional.empty());

        mockMvc.perform(get("/api/crypto/ETHUSDT/live"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("LIVE_DATA_NOT_AVAILABLE"))
                .andExpect(jsonPath("$.message").value("No live update has been received yet for ETHUSDT"));
    }

    @Test
    void unsubscribedSymbolReturns404() throws Exception {
        when(store.get("DOGEUSDT")).thenReturn(Optional.empty());

        mockMvc.perform(get("/api/crypto/dogeusdt/live"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("LIVE_DATA_NOT_AVAILABLE"))
                .andExpect(jsonPath("$.message").value("Symbol DOGEUSDT is not part of the live subscriptions"));
    }

    @Test
    void malformedSymbolReturns400WithoutTouchingStore() throws Exception {
        mockMvc.perform(get("/api/crypto/BTC-USDT/live"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));

        verifyNoInteractions(store);
    }
}
