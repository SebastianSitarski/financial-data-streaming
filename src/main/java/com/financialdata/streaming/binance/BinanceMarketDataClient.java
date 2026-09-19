package com.financialdata.streaming.binance;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.financialdata.streaming.binance.dto.BinanceApiError;
import com.financialdata.streaming.binance.dto.BinanceExchangeInfoResponse;
import com.financialdata.streaming.binance.dto.BinancePriceResponse;
import com.financialdata.streaming.binance.dto.BinanceSymbolInfo;
import com.financialdata.streaming.binance.dto.BinanceTicker24hResponse;
import com.financialdata.streaming.market.Candle;
import com.financialdata.streaming.market.CandleInterval;
import com.financialdata.streaming.market.CryptoMarketStats;
import com.financialdata.streaming.market.CryptoPrice;
import com.financialdata.streaming.market.CryptoSymbolNotFoundException;
import com.financialdata.streaming.market.MarketDataProvider;
import com.financialdata.streaming.market.MarketDataRateLimitedException;
import com.financialdata.streaming.market.MarketDataUnavailableException;
import com.financialdata.streaming.market.SymbolInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

/**
 * Synchronous adapter for the Binance public (key-less) Spot market-data REST API.
 * Everything Binance-specific (endpoints, DTOs, error codes, kline layout) ends here;
 * callers only see application models and application exceptions.
 *
 * <p>Binance rate limits are enforced per IP, so one caller exhausting the budget affects every
 * caller of this service. After a 429/418 the client fails fast (without contacting Binance) until
 * the {@code Retry-After} deadline has passed; hammering Binance during a ban only prolongs it.
 */
@Component
public class BinanceMarketDataClient implements MarketDataProvider {

    /** Binance returns 418 once a client keeps calling after being rate limited (temporary IP ban). */
    private static final int IP_BANNED = 418;

    /** Back-off applied when Binance rate limits us without saying for how long. */
    static final Duration DEFAULT_RATE_LIMIT_BACKOFF = Duration.ofSeconds(5);

    private static final ParameterizedTypeReference<List<List<Object>>> RAW_KLINES = new ParameterizedTypeReference<>() {
    };

    private final RestClient restClient;
    private final BinanceKlineMapper klineMapper;
    private final Clock clock;
    private final AtomicReference<Instant> blockedUntil = new AtomicReference<>(Instant.EPOCH);

    @Autowired
    public BinanceMarketDataClient(RestClient binanceRestClient, BinanceKlineMapper klineMapper) {
        this(binanceRestClient, klineMapper, Clock.systemUTC());
    }

    BinanceMarketDataClient(RestClient binanceRestClient, BinanceKlineMapper klineMapper, Clock clock) {
        this.restClient = binanceRestClient;
        this.klineMapper = klineMapper;
        this.clock = clock;
    }

    @Override
    public CryptoPrice getPrice(String symbol) {
        BinancePriceResponse price = execute(symbol, () -> restClient.get()
                .uri("/api/v3/ticker/price?symbol={symbol}", symbol)
                .retrieve()
                .body(BinancePriceResponse.class));
        return new CryptoPrice(price.symbol(), price.price());
    }

    @Override
    public SymbolInfo getSymbolInfo(String symbol) {
        BinanceExchangeInfoResponse response = execute(symbol, () -> restClient.get()
                .uri("/api/v3/exchangeInfo?symbol={symbol}", symbol)
                .retrieve()
                .body(BinanceExchangeInfoResponse.class));
        if (response.symbols() == null || response.symbols().isEmpty()) {
            throw new CryptoSymbolNotFoundException(symbol);
        }
        BinanceSymbolInfo info = response.symbols().getFirst();
        return new SymbolInfo(info.symbol(), info.baseAsset(), info.quoteAsset(), info.status());
    }

    @Override
    public CryptoMarketStats getStats(String symbol) {
        BinanceTicker24hResponse ticker = execute(symbol, () -> restClient.get()
                .uri("/api/v3/ticker/24hr?symbol={symbol}", symbol)
                .retrieve()
                .body(BinanceTicker24hResponse.class));
        return new CryptoMarketStats(
                ticker.symbol(),
                ticker.lastPrice(),
                ticker.priceChange(),
                ticker.priceChangePercent(),
                ticker.highPrice(),
                ticker.lowPrice(),
                ticker.volume());
    }

    @Override
    public List<Candle> getCandles(String symbol, CandleInterval interval, int limit) {
        List<List<Object>> rawKlines = execute(symbol, () -> restClient.get()
                .uri("/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}",
                        symbol, interval.code(), limit)
                .retrieve()
                .body(RAW_KLINES));
        try {
            return klineMapper.toCandles(rawKlines);
        }
        catch (IllegalArgumentException ex) {
            // kline payload that does not match the documented layout
            throw new MarketDataUnavailableException("Unexpected payload from Binance", ex);
        }
    }

    /**
     * Runs a Binance call and translates every failure mode into an application exception, so a
     * misbehaving upstream never surfaces as an unhandled 500 to API clients.
     */
    private <T> T execute(String symbol, Supplier<T> call) {
        failFastWhileRateLimited();
        T result;
        try {
            result = call.get();
        }
        catch (RestClientResponseException ex) {
            RuntimeException translated = translate(symbol, ex);
            if (translated instanceof MarketDataRateLimitedException rateLimited) {
                blockFor(rateLimited.getRetryAfter().orElse(DEFAULT_RATE_LIMIT_BACKOFF));
            }
            throw translated;
        }
        catch (ResourceAccessException ex) {
            throw new MarketDataUnavailableException("Binance is unreachable", ex);
        }
        catch (RestClientException ex) {
            // decoding failures: unexpected content type, malformed JSON, wrong field types
            throw new MarketDataUnavailableException("Unexpected response from Binance", ex);
        }
        if (result == null) {
            throw new MarketDataUnavailableException("Empty response from Binance", null);
        }
        return result;
    }

    private void failFastWhileRateLimited() {
        Instant now = clock.instant();
        Instant until = blockedUntil.get();
        if (now.isBefore(until)) {
            // whole seconds, rounded up, so the Retry-After we forward never says "0"
            long remainingSeconds = Math.max(1, Duration.between(now, until).plusMillis(999).toSeconds());
            throw new MarketDataRateLimitedException(Duration.ofSeconds(remainingSeconds), null);
        }
    }

    private void blockFor(Duration backoff) {
        Instant until = clock.instant().plus(backoff);
        // never shorten a deadline set by a concurrent request that saw a longer Retry-After
        blockedUntil.accumulateAndGet(until, (current, candidate) -> current.isAfter(candidate) ? current : candidate);
    }

    private static RuntimeException translate(String symbol, RestClientResponseException ex) {
        int status = ex.getStatusCode().value();
        if (status == HttpStatus.TOO_MANY_REQUESTS.value() || status == IP_BANNED) {
            return new MarketDataRateLimitedException(retryAfter(ex.getResponseHeaders()), ex);
        }
        if (ex.getStatusCode().is4xxClientError() && isJson(ex)) {
            BinanceApiError error = parseError(ex);
            if (error != null && error.code() == BinanceApiError.INVALID_SYMBOL) {
                return new CryptoSymbolNotFoundException(symbol);
            }
        }
        return new MarketDataUnavailableException("Binance responded with HTTP " + status, ex);
    }

    private static BinanceApiError parseError(RestClientResponseException ex) {
        try {
            return ex.getResponseBodyAs(BinanceApiError.class);
        }
        catch (RestClientException | IllegalStateException parseFailure) {
            // JSON content type but unparseable body (e.g. proxy error page); treat as unknown error
            return null;
        }
    }

    private static Duration retryAfter(HttpHeaders headers) {
        String value = headers != null ? headers.getFirst(HttpHeaders.RETRY_AFTER) : null;
        if (value == null) {
            return null;
        }
        try {
            return Duration.ofSeconds(Long.parseLong(value.strip()));
        }
        catch (NumberFormatException notSeconds) {
            // HTTP-date form is allowed by the spec but Binance sends seconds; ignore anything else
            return null;
        }
    }

    private static boolean isJson(RestClientResponseException ex) {
        MediaType contentType = ex.getResponseHeaders() != null ? ex.getResponseHeaders().getContentType() : null;
        return contentType != null && MediaType.APPLICATION_JSON.isCompatibleWith(contentType);
    }
}
