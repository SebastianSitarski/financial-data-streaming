package com.financialdata.streaming.market;

import java.util.List;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/crypto")
public class CryptoMarketController {

    static final int MAX_CANDLE_LIMIT = 1000;

    private final CryptoMarketService marketService;

    public CryptoMarketController(CryptoMarketService marketService) {
        this.marketService = marketService;
    }

    @GetMapping("/{symbol}")
    public CryptoMarket getMarket(@PathVariable String symbol) {
        return marketService.getMarket(symbol);
    }

    @GetMapping("/{symbol}/price")
    public CryptoPrice getPrice(@PathVariable String symbol) {
        return marketService.getPrice(symbol);
    }

    @GetMapping("/{symbol}/stats")
    public CryptoMarketStats getStats(@PathVariable String symbol) {
        return marketService.getStats(symbol);
    }

    @GetMapping("/{symbol}/candles")
    public List<Candle> getCandles(
            @PathVariable String symbol,
            @RequestParam(defaultValue = "1h") String interval,
            @RequestParam(defaultValue = "100") @Min(1) @Max(MAX_CANDLE_LIMIT) int limit) {
        return marketService.getCandles(symbol, CandleInterval.fromCode(interval), limit);
    }
}
