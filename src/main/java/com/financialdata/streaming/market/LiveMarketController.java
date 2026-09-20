package com.financialdata.streaming.market;

import com.financialdata.streaming.stream.LatestMarketDataStore;
import com.financialdata.streaming.stream.MarketDataProperties;
import com.financialdata.streaming.stream.MarketUpdate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Exposes the latest state that travelled Binance WebSocket → Kafka → consumer. Deliberately separate
 * from {@link CryptoMarketController}: this endpoint never calls Binance REST and must not fall back
 * to it, otherwise a broken pipeline would go unnoticed.
 */
@RestController
@RequestMapping("/api/crypto")
public class LiveMarketController {

    private final LatestMarketDataStore store;
    private final MarketDataProperties properties;

    public LiveMarketController(LatestMarketDataStore store, MarketDataProperties properties) {
        this.store = store;
        this.properties = properties;
    }

    @GetMapping("/{symbol}/live")
    public MarketUpdate getLive(@PathVariable String symbol) {
        String normalized = CryptoMarketService.normalize(symbol);
        return store.get(normalized).orElseThrow(() -> properties.symbols().contains(normalized)
                ? LiveMarketDataNotAvailableException.noUpdateYet(normalized)
                : LiveMarketDataNotAvailableException.notSubscribed(normalized));
    }
}
