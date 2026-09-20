# financial-data-streaming

Spring Boot service that exposes cryptocurrency market data sourced from the
[Binance public Spot REST API](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/general-api-information)
(market-data host `https://data-api.binance.vision`, no API key required).
Kafka is provisioned in the local Docker environment for the upcoming streaming stage
(Binance WebSocket → Kafka), but is not used by the REST features yet.

## Stack

- Java 21, Maven (wrapper included)
- Spring Boot 4.1.1 — Web MVC, RestClient, Validation, Actuator, Kafka
- Apache Kafka 4.1 (KRaft, single node) via Docker Compose

## Getting started

```bash
# build and run tests
./mvnw verify

# run the application (port 8080)
./mvnw spring-boot:run

# optional: start Kafka for the later streaming stage
docker compose up -d
```

The REST endpoints work without a running Kafka broker.

## Configuration

`src/main/resources/application.yml`

| Property                  | Default                            | Description                        |
|---------------------------|------------------------------------|------------------------------------|
| `binance.base-url`        | `https://data-api.binance.vision`  | Binance public market-data API     |
| `binance.connect-timeout` | `5s`                               | HTTP connect timeout               |
| `binance.read-timeout`    | `10s`                              | HTTP read timeout                  |
| `binance.ws-url`          | `wss://stream.binance.com:9443/ws` | Reserved for the WebSocket stage   |
| `spring.kafka.*`          | `localhost:9092`                   | Reserved for the Kafka stage       |

## REST API

Symbols are case-insensitive (`btcusdt`, `BtcUsdt` and `BTCUSDT` are equivalent) and
Binance is the source of truth for which symbols exist.

| Endpoint                                              | Description                              | Binance source                          |
|-------------------------------------------------------|------------------------------------------|-----------------------------------------|
| `GET /api/crypto/{symbol}`                            | Symbol metadata combined with last price | `/api/v3/exchangeInfo`, `/api/v3/ticker/price` |
| `GET /api/crypto/{symbol}/price`                      | Current price                            | `/api/v3/ticker/price`                  |
| `GET /api/crypto/{symbol}/stats`                      | 24-hour statistics                       | `/api/v3/ticker/24hr`                   |
| `GET /api/crypto/{symbol}/candles?interval=1h&limit=100` | Historical candles (OHLCV)            | `/api/v3/klines`                        |

Candle parameters: `interval` — one of `1s 1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1M`
(default `1h`); `limit` — `1..1000` (default `100`).

### Examples

```bash
curl localhost:8080/api/crypto/BTCUSDT/price
# {"symbol":"BTCUSDT","price":81262.01000000}

curl localhost:8080/api/crypto/btcusdt
# {"symbol":"BTCUSDT","baseAsset":"BTC","quoteAsset":"USDT","status":"TRADING","price":81262.00000000}

curl localhost:8080/api/crypto/ETHUSDT/stats
# {"symbol":"ETHUSDT","lastPrice":2639.29000000,"priceChange":128.47000000,"priceChangePercent":5.117,
#  "highPrice":2662.85000000,"lowPrice":2494.37000000,"volume":437496.77020000}

curl "localhost:8080/api/crypto/SOLUSDT/candles?interval=1h&limit=2"
# [{"openTime":"2026-09-19T10:00:00Z","open":111.84000000,"high":112.14000000,"low":111.63000000,
#   "close":111.63000000,"volume":61718.87000000}, ...]
```

All monetary values are serialized as JSON numbers backed by `BigDecimal`; timestamps are ISO-8601 (UTC).

### Errors

Every error — including Spring MVC's own (unknown path, wrong method) — uses the same body:
`{"code": "...", "message": "..."}`.

| HTTP | `code`                     | When                                                                       |
|------|----------------------------|----------------------------------------------------------------------------|
| 400  | `INVALID_REQUEST`          | Malformed symbol, unsupported interval, `limit` out of range or not a number (message names the parameter) |
| 404  | `CRYPTO_SYMBOL_NOT_FOUND`  | Binance does not know the symbol                                           |
| 404  | `NOT_FOUND`                | Unknown path                                                               |
| 405  | `METHOD_NOT_ALLOWED`       | Unsupported HTTP method                                                    |
| 429  | `MARKET_DATA_RATE_LIMITED` | Binance rate limit (429) or temporary IP ban (418); Binance's `Retry-After` is forwarded when present. No automatic retry. Until that deadline passes (5 s when Binance sent none) every request fails fast with the remaining `Retry-After` **without** calling Binance — the limit is per IP and shared by all callers, and hammering Binance during a ban only prolongs it |
| 502  | `MARKET_DATA_UNAVAILABLE`  | Binance unreachable, timed out, returned 5xx / unexpected 4xx, or a response that could not be decoded |
| 500  | `INTERNAL_ERROR`           | Unexpected application failure                                             |

Raw Binance error payloads are never returned to API clients. Upstream failures are logged as a
single `WARN` line (full stack trace at `DEBUG`) so an outage does not flood the logs.

## Postman

`postman/` contains a collection and a local environment covering the main success,
normalization and error cases, each with response assertions:

- `financial-data-streaming.postman_collection.json`
- `local.postman_environment.json` (`baseUrl=http://localhost:8080`, `symbol=BTCUSDT`)

Import both into Postman and run the collection against a running instance, or use Newman:

```bash
newman run postman/financial-data-streaming.postman_collection.json \
  -e postman/local.postman_environment.json
```

## Project layout

```
com.financialdata.streaming
├── market    Controller, service, application models (records), MarketDataProvider port, domain exceptions
├── binance   BinanceMarketDataClient (implements MarketDataProvider), transport DTOs, kline mapper, config
└── web       Shared error response and @RestControllerAdvice
```

`market` knows nothing about Binance: the service depends on the `MarketDataProvider` interface,
and the Binance adapter maps its transport DTOs into application models before they leave the
`binance` package. Adding another data source (e.g. the WebSocket stream) means another adapter,
not changes in `market`.

### Known limitations / next steps

- Every API call is forwarded to Binance (`GET /api/crypto/{symbol}` makes two calls). There is no
  cache or client-side throttling yet, so heavy traffic can exhaust Binance's per-IP request weight;
  metadata from `exchangeInfo` is the first candidate for caching.

## Roadmap

1. Binance WebSocket trade stream → Kafka producer (`crypto.trades` topic)
2. Consumers / processing on top of Kafka
