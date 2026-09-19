package com.financialdata.streaming.binance;

import java.math.BigDecimal;
import java.net.SocketTimeoutException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;

import com.financialdata.streaming.market.Candle;
import com.financialdata.streaming.market.CandleInterval;
import com.financialdata.streaming.market.CryptoMarketStats;
import com.financialdata.streaming.market.CryptoPrice;
import com.financialdata.streaming.market.CryptoSymbolNotFoundException;
import com.financialdata.streaming.market.MarketDataRateLimitedException;
import com.financialdata.streaming.market.MarketDataUnavailableException;
import com.financialdata.streaming.market.SymbolInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.test.web.client.ResponseActions;
import org.springframework.web.client.RestClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.springframework.http.HttpMethod.GET;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withException;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withTooManyRequests;

class BinanceMarketDataClientTest {

    private static final String BASE_URL = "https://binance.test";
    private static final String PRICE_URL = BASE_URL + "/api/v3/ticker/price?symbol=BTCUSDT";
    private static final String INVALID_SYMBOL_BODY = "{\"code\":-1121,\"msg\":\"Invalid symbol.\"}";

    private MockRestServiceServer server;
    private MutableClock clock;
    private BinanceMarketDataClient client;

    @BeforeEach
    void setUp() {
        RestClient.Builder builder = RestClient.builder().baseUrl(BASE_URL);
        server = MockRestServiceServer.bindTo(builder).build();
        clock = new MutableClock(Instant.parse("2026-09-20T10:00:00Z"));
        client = new BinanceMarketDataClient(builder.build(), new BinanceKlineMapper(), clock);
    }

    @AfterEach
    void verifyAllExpectedRequestsWereMade() {
        server.verify();
    }

    @Test
    void getPriceMapsToApplicationModel() {
        expectGet(PRICE_URL).andRespond(json("{\"symbol\":\"BTCUSDT\",\"price\":\"81266.00000000\"}"));

        assertThat(client.getPrice("BTCUSDT"))
                .isEqualTo(new CryptoPrice("BTCUSDT", new BigDecimal("81266.00000000")));
    }

    @Test
    void getSymbolInfoMapsFirstSymbolFromExchangeInfo() {
        expectGet(BASE_URL + "/api/v3/exchangeInfo?symbol=BTCUSDT").andRespond(json("""
                {"timezone":"UTC","serverTime":1789817455004,"rateLimits":[],"exchangeFilters":[],
                 "symbols":[{"symbol":"BTCUSDT","status":"TRADING","baseAsset":"BTC","baseAssetPrecision":8,
                             "quoteAsset":"USDT","filters":[],"permissions":[]}]}
                """));

        assertThat(client.getSymbolInfo("BTCUSDT")).isEqualTo(new SymbolInfo("BTCUSDT", "BTC", "USDT", "TRADING"));
    }

    @Test
    void getSymbolInfoThrowsNotFoundWhenSymbolsListIsEmpty() {
        expectGet(BASE_URL + "/api/v3/exchangeInfo?symbol=BTCUSDT").andRespond(json("{\"symbols\":[]}"));

        assertThatExceptionOfType(CryptoSymbolNotFoundException.class)
                .isThrownBy(() -> client.getSymbolInfo("BTCUSDT"));
    }

    @Test
    void getSymbolInfoTranslatesInvalidSymbolError() {
        expectGet(BASE_URL + "/api/v3/exchangeInfo?symbol=BTCXYZ")
                .andRespond(withStatus(HttpStatus.BAD_REQUEST)
                        .contentType(MediaType.APPLICATION_JSON).body(INVALID_SYMBOL_BODY));

        assertThatExceptionOfType(CryptoSymbolNotFoundException.class)
                .isThrownBy(() -> client.getSymbolInfo("BTCXYZ"));
    }

    @Test
    void getStatsMapsRequiredFieldsOnly() {
        expectGet(BASE_URL + "/api/v3/ticker/24hr?symbol=BTCUSDT").andRespond(json("""
                {"symbol":"BTCUSDT","priceChange":"3217.20000000","priceChangePercent":"4.122",
                 "weightedAvgPrice":"80000.1","prevClosePrice":"78048.8","lastPrice":"81266.01000000",
                 "lastQty":"0.001","bidPrice":"81266.00","askPrice":"81266.01","openPrice":"78048.81",
                 "highPrice":"81741.00000000","lowPrice":"77972.30000000","volume":"19525.89403000",
                 "quoteVolume":"1574155594.19","openTime":1789731055004,"closeTime":1789817455004,
                 "firstId":1,"lastId":2,"count":2}
                """));

        assertThat(client.getStats("BTCUSDT")).isEqualTo(new CryptoMarketStats(
                "BTCUSDT",
                new BigDecimal("81266.01000000"),
                new BigDecimal("3217.20000000"),
                new BigDecimal("4.122"),
                new BigDecimal("81741.00000000"),
                new BigDecimal("77972.30000000"),
                new BigDecimal("19525.89403000")));
    }

    @Test
    void getCandlesMapsRawArraysAndUsesIntervalCode() {
        expectGet(BASE_URL + "/api/v3/klines?symbol=BTCUSDT&interval=1h&limit=2").andRespond(json("""
                [[1789812000000,"81325.38000000","81330.29000000","81199.99000000","81216.70000000","532.29651000",
                  1789815599999,"43260047.65814100",50225,"247.98007000","20150816.54100580","0"],
                 [1789815600000,"81216.71000000","81374.15000000","81212.98000000","81266.00000000","189.65367000",
                  1789819199999,"15419415.66929920",19864,"110.54896000","8987058.45145410","0"]]
                """));

        List<Candle> candles = client.getCandles("BTCUSDT", CandleInterval.ONE_HOUR, 2);

        assertThat(candles).hasSize(2);
        assertThat(candles.getFirst()).isEqualTo(new Candle(
                Instant.ofEpochMilli(1789812000000L),
                new BigDecimal("81325.38000000"),
                new BigDecimal("81330.29000000"),
                new BigDecimal("81199.99000000"),
                new BigDecimal("81216.70000000"),
                new BigDecimal("532.29651000")));
    }

    @Test
    void malformedKlinePayloadIsTranslatedToUnavailable() {
        expectGet(BASE_URL + "/api/v3/klines?symbol=BTCUSDT&interval=1h&limit=1")
                .andRespond(json("[[1789812000000,\"81325.38\",\"81330.29\"]]"));

        assertThatExceptionOfType(MarketDataUnavailableException.class)
                .isThrownBy(() -> client.getCandles("BTCUSDT", CandleInterval.ONE_HOUR, 1))
                .withMessage("Unexpected payload from Binance");
    }

    @Test
    void invalidSymbolErrorIsTranslatedToSymbolNotFound() {
        expectGet(BASE_URL + "/api/v3/ticker/price?symbol=BTCXYZ")
                .andRespond(withStatus(HttpStatus.BAD_REQUEST)
                        .contentType(MediaType.APPLICATION_JSON).body(INVALID_SYMBOL_BODY));

        assertThatExceptionOfType(CryptoSymbolNotFoundException.class)
                .isThrownBy(() -> client.getPrice("BTCXYZ"))
                .withMessage("Symbol BTCXYZ was not found");
    }

    @Test
    void otherBinanceClientErrorsAreTranslatedToUnavailable() {
        expectGet(PRICE_URL).andRespond(withStatus(HttpStatus.BAD_REQUEST)
                .contentType(MediaType.APPLICATION_JSON)
                .body("{\"code\":-1100,\"msg\":\"Illegal characters found in parameter 'symbol'\"}"));

        assertThatExceptionOfType(MarketDataUnavailableException.class)
                .isThrownBy(() -> client.getPrice("BTCUSDT"))
                .withMessage("Binance responded with HTTP 400");
    }

    @Test
    void clientErrorWithoutJsonBodyIsTranslatedToUnavailable() {
        expectGet(PRICE_URL).andRespond(withStatus(HttpStatus.FORBIDDEN)
                .contentType(MediaType.TEXT_HTML).body("<html>blocked by WAF</html>"));

        assertThatExceptionOfType(MarketDataUnavailableException.class)
                .isThrownBy(() -> client.getPrice("BTCUSDT"))
                .withMessage("Binance responded with HTTP 403");
    }

    @Test
    void clientErrorWithUnparseableJsonBodyIsTranslatedToUnavailable() {
        expectGet(PRICE_URL).andRespond(withStatus(HttpStatus.BAD_REQUEST)
                .contentType(MediaType.APPLICATION_JSON).body("<html>not json</html>"));

        assertThatExceptionOfType(MarketDataUnavailableException.class)
                .isThrownBy(() -> client.getPrice("BTCUSDT"))
                .withMessage("Binance responded with HTTP 400");
    }

    @Test
    void upstreamServerErrorIsTranslatedToUnavailable() {
        expectGet(BASE_URL + "/api/v3/ticker/24hr?symbol=BTCUSDT")
                .andRespond(withStatus(HttpStatus.SERVICE_UNAVAILABLE).body("upstream down"));

        assertThatExceptionOfType(MarketDataUnavailableException.class)
                .isThrownBy(() -> client.getStats("BTCUSDT"))
                .withMessage("Binance responded with HTTP 503");
    }

    @Test
    void successWithNonJsonBodyIsTranslatedToUnavailable() {
        expectGet(PRICE_URL).andRespond(withSuccess("<html>maintenance</html>", MediaType.TEXT_HTML));

        assertThatExceptionOfType(MarketDataUnavailableException.class)
                .isThrownBy(() -> client.getPrice("BTCUSDT"))
                .withMessage("Unexpected response from Binance");
    }

    @Test
    void successWithMalformedJsonIsTranslatedToUnavailable() {
        expectGet(PRICE_URL).andRespond(json("{\"symbol\":\"BTCUSDT\",\"price\":"));

        assertThatExceptionOfType(MarketDataUnavailableException.class)
                .isThrownBy(() -> client.getPrice("BTCUSDT"))
                .withMessage("Unexpected response from Binance");
    }

    @Test
    void ioFailureIsTranslatedToUnavailable() {
        expectGet(PRICE_URL).andRespond(withException(new SocketTimeoutException("read timed out")));

        assertThatExceptionOfType(MarketDataUnavailableException.class)
                .isThrownBy(() -> client.getPrice("BTCUSDT"))
                .withMessage("Binance is unreachable");
    }

    @Test
    void tooManyRequestsIsTranslatedToRateLimitedWithRetryAfter() {
        expectGet(PRICE_URL).andRespond(withTooManyRequests()
                .header(HttpHeaders.RETRY_AFTER, "42")
                .contentType(MediaType.APPLICATION_JSON)
                .body("{\"code\":-1003,\"msg\":\"Too many requests.\"}"));

        assertThatExceptionOfType(MarketDataRateLimitedException.class)
                .isThrownBy(() -> client.getPrice("BTCUSDT"))
                .satisfies(ex -> assertThat(ex.getRetryAfter()).contains(Duration.ofSeconds(42)));
    }

    @Test
    void tooManyRequestsWithoutRetryAfterHasEmptyHint() {
        expectGet(PRICE_URL).andRespond(withTooManyRequests());

        assertThatExceptionOfType(MarketDataRateLimitedException.class)
                .isThrownBy(() -> client.getPrice("BTCUSDT"))
                .satisfies(ex -> assertThat(ex.getRetryAfter()).isEmpty());
    }

    @Test
    void ipBanIsTranslatedToRateLimited() {
        expectGet(PRICE_URL).andRespond(withStatus(HttpStatus.I_AM_A_TEAPOT)
                .header(HttpHeaders.RETRY_AFTER, "120")
                .contentType(MediaType.APPLICATION_JSON)
                .body("{\"code\":-1003,\"msg\":\"Way too much request weight used; IP banned\"}"));

        assertThatExceptionOfType(MarketDataRateLimitedException.class)
                .isThrownBy(() -> client.getPrice("BTCUSDT"))
                .satisfies(ex -> assertThat(ex.getRetryAfter()).contains(Duration.ofSeconds(120)));
    }

    @Test
    void afterRateLimitSubsequentCallsFailFastWithoutContactingBinance() {
        // exactly one upstream request is expected; server.verify() in @AfterEach fails on a second one
        expectGet(PRICE_URL).andRespond(withTooManyRequests().header(HttpHeaders.RETRY_AFTER, "42"));

        assertThatExceptionOfType(MarketDataRateLimitedException.class).isThrownBy(() -> client.getPrice("BTCUSDT"));
        clock.advance(Duration.ofSeconds(10));

        assertThatExceptionOfType(MarketDataRateLimitedException.class)
                .isThrownBy(() -> client.getStats("BTCUSDT"))
                .satisfies(ex -> assertThat(ex.getRetryAfter()).contains(Duration.ofSeconds(32)));
    }

    @Test
    void callsResumeOnceRetryAfterHasElapsed() {
        expectGet(PRICE_URL).andRespond(withTooManyRequests().header(HttpHeaders.RETRY_AFTER, "42"));
        expectGet(PRICE_URL).andRespond(json("{\"symbol\":\"BTCUSDT\",\"price\":\"1\"}"));

        assertThatExceptionOfType(MarketDataRateLimitedException.class).isThrownBy(() -> client.getPrice("BTCUSDT"));
        clock.advance(Duration.ofSeconds(42));

        assertThat(client.getPrice("BTCUSDT").price()).isEqualByComparingTo("1");
    }

    @Test
    void rateLimitWithoutRetryAfterUsesDefaultBackoff() {
        expectGet(PRICE_URL).andRespond(withTooManyRequests());

        assertThatExceptionOfType(MarketDataRateLimitedException.class).isThrownBy(() -> client.getPrice("BTCUSDT"));

        assertThatExceptionOfType(MarketDataRateLimitedException.class)
                .isThrownBy(() -> client.getPrice("BTCUSDT"))
                .satisfies(ex -> assertThat(ex.getRetryAfter())
                        .contains(BinanceMarketDataClient.DEFAULT_RATE_LIMIT_BACKOFF));
    }

    @Test
    void ipBanBlocksSubsequentCallsAsWell() {
        expectGet(PRICE_URL).andRespond(withStatus(HttpStatus.I_AM_A_TEAPOT).header(HttpHeaders.RETRY_AFTER, "120"));

        assertThatExceptionOfType(MarketDataRateLimitedException.class).isThrownBy(() -> client.getPrice("BTCUSDT"));

        assertThatExceptionOfType(MarketDataRateLimitedException.class)
                .isThrownBy(() -> client.getCandles("BTCUSDT", CandleInterval.ONE_HOUR, 1));
    }

    @Test
    void remainingBackoffIsRoundedUpToWholeSeconds() {
        expectGet(PRICE_URL).andRespond(withTooManyRequests().header(HttpHeaders.RETRY_AFTER, "2"));

        assertThatExceptionOfType(MarketDataRateLimitedException.class).isThrownBy(() -> client.getPrice("BTCUSDT"));
        clock.advance(Duration.ofMillis(1500));

        assertThatExceptionOfType(MarketDataRateLimitedException.class)
                .isThrownBy(() -> client.getPrice("BTCUSDT"))
                .satisfies(ex -> assertThat(ex.getRetryAfter()).contains(Duration.ofSeconds(1)));
    }

    private ResponseActions expectGet(String url) {
        return server.expect(requestTo(url)).andExpect(method(GET));
    }

    private static org.springframework.test.web.client.ResponseCreator json(String body) {
        return withSuccess(body, MediaType.APPLICATION_JSON);
    }

    /** Test clock that only moves when the test says so. */
    private static final class MutableClock extends Clock {

        private Instant now;

        MutableClock(Instant start) {
            this.now = start;
        }

        void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }
}
