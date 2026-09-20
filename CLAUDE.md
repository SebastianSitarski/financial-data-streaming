# Project Engineering Guide

This is an existing Java backend repository.

The project uses:

* Java 21
* Spring Boot 4.1.1 (Maven, `./mvnw`) — **not** Spring Boot 3.x: starter names, `RestClient`
  auto-configuration, Jackson 3 and test annotations (`@MockitoBean`, not `@MockBean`) differ
* Docker-based local infrastructure (`docker-compose.yml`)
* Kafka is already available in Docker and will be used in later development stages
* external financial/market-data integrations, currently the Binance public REST API

Always inspect the actual repository before making assumptions about exact dependencies, package
structure, configuration, build tooling, or existing conventions.

## Repository facts (verified — re-check if they look stale)

```
com.financialdata.streaming
├── market            REST controllers (snapshot + /live), service, application models, domain exceptions
├── binance           BinanceMarketDataClient (REST adapter, implements MarketDataProvider), DTOs, kline mapper
│   └── stream        BinanceMarketStreamClient (WebSocket, SmartLifecycle, reconnect), ticker DTOs, mapper
├── stream            MarketUpdate event, MarketUpdatePublisher/Listener, NewTopic config, LatestMarketDataStore
└── web               Shared error response and @RestControllerAdvice
```

* `market` knows nothing about Binance; new data sources are new adapters, not changes in `market`.
* Live pipeline: Binance combined `@ticker` WebSocket → `MarketUpdate` → Kafka `crypto.market-updates`
  (key = symbol, at-least-once) → `LatestMarketDataStore` → `GET /api/crypto/{symbol}/live`.
* Kafka JSON uses Spring Kafka's **Jackson 3** `JacksonJsonSerializer`/`JacksonJsonDeserializer`
  (wrapped in `ErrorHandlingDeserializer`), without type headers: the consumer's value type comes from
  `spring.json.value.default.type`. The older `JsonSerializer`/`JsonDeserializer` need Jackson 2, which is
  only on the *test* classpath — do not use them.
* The live consumer uses `auto.offset.reset=latest` and the topic has a 1h retention: it is a latest-state
  pipeline, replaying history is never desirable.
* WebSocket client: Spring `StandardWebSocketClient` over embedded Tomcat (already in `pom.xml`); the
  stream is disabled in tests via `binance.stream.enabled=false`.
* Tests: plain JUnit 5 + AssertJ for logic, `@WebMvcTest` for controllers, `MockRestServiceServer`
  for the Binance REST client, mocked `WebSocketClient`/`TaskScheduler` + controllable `Clock` for the
  stream client. `@SpringBootTest` (+`@EmbeddedKafka`) is used only for the context-loads smoke test.
* Configuration lives in `src/main/resources/application.yml` (`binance.*` bound via
  `@ConfigurationProperties` record `BinanceProperties`).
* Postman collection in `postman/`.

Specialized skills exist for review and refactoring work: `java-code-review`, `spring-backend-review`,
`kafka-review`, `java-refactor`, `java-performance`. This file is the project baseline; the skills
carry the detailed checklists.

---

# Core engineering principles

Prioritize work in this order:

1. Correctness
2. Data integrity
3. Reliability
4. Maintainability
5. Performance
6. Testability
7. Simplicity
8. Style

Correctness always takes precedence over performance or stylistic improvements.

Prefer simple, explicit solutions over clever abstractions. Do not overengineer.

---

# Repository-first rule

Before modifying code, inspect the relevant repository context. At minimum inspect:

* target implementation
* callers and usages
* interfaces
* related domain models
* tests
* configuration
* build configuration
* framework setup where relevant

Do not infer behavior from a single method when callers or configuration may affect it.

Never invent classes, methods, APIs, dependencies, configuration properties, beans, topics, database
tables, endpoints or framework behavior. If something cannot be verified from the repository, state
that explicitly. Do not present assumptions as facts.

---

# Before making changes

For non-trivial tasks:

1. inspect the relevant code
2. understand the existing behavior
3. identify dependencies and callers
4. inspect tests
5. propose a short implementation plan
6. make the smallest coherent change
7. add or update focused tests
8. run verification
9. inspect the final diff

Do not start implementing a substantial change before understanding how the existing code works.

---

# Scope discipline

Make only changes required for the task. Do not perform unrelated cleanup. Do not refactor unrelated
modules simply because they could be improved.

If unrelated issues are discovered, mention them separately; do not silently expand task scope.

Preserve existing public behavior unless the task explicitly requires changing it.

---

# Java conventions

Use Java 21 capabilities where they improve clarity.

Prefer:

* immutable objects
* constructor injection
* records for simple immutable DTOs where appropriate
* enums for bounded domain values
* clear domain-specific names
* small focused methods
* explicit behavior over clever constructs

Avoid:

* mutable global/static state
* unnecessary inheritance
* utility classes without a strong reason
* excessive generic abstractions
* interfaces with only one implementation unless they provide a concrete benefit
* builders for trivial objects
* factories for simple construction
* unnecessary wrappers
* unnecessary `Optional` usage in fields or method parameters

Use `Optional` mainly for return values representing optional results where that improves the API.
Do not use `Optional.get()` without proving a value is present.

---

# Financial data

This project processes financial and market data.

Never use `double` or `float` for prices, monetary values, quantities, percentages, or other values
where decimal precision matters. Prefer `BigDecimal` unless the external protocol or an existing
internal model requires something else.

Be explicit about precision, scale and rounding when calculations require them. Do not introduce
rounding arbitrarily. Compare `BigDecimal` values with `compareTo`, not `equals` (scale-sensitive).

---

# Collections and algorithms

Choose data structures based on how they are used. Pay attention to:

* repeated linear scans
* nested loops
* repeated sorting
* unnecessary copying
* unnecessary intermediate collections
* repeated parsing/conversion
* allocation in frequently executed paths

If repeated lookup by key is required, consider whether a `Map<K, V>` is more appropriate than
repeatedly scanning a `List<V>`. Do not optimize without considering expected data size and execution
frequency.

---

# Streams

Java Streams are allowed, but readability and performance matter. Use streams when they make code
clearer. Prefer loops when the stream becomes difficult to understand, processing is
performance-sensitive, multiple intermediate collections are created unnecessarily, or control flow is
complex.

Do not rewrite readable loops into streams only for stylistic reasons. Do not rewrite readable streams
into loops unless there is a concrete benefit.

---

# Spring Boot

Follow existing repository conventions. Prefer constructor injection; avoid field injection.

Spring beans are singleton by default. Do not store mutable request-specific state inside singleton
`@Service`, `@Component` or `@Controller` beans.

When reviewing or implementing Spring code, consider bean lifecycle, proxy behavior, transaction
boundaries, concurrency, HTTP client lifecycle, configuration, exception translation and resource
management.

---

# Transactions

Do not add `@Transactional` casually. Before using or modifying a transaction, understand the
transaction boundary, propagation, rollback behavior, database operations involved, calls to external
systems, and Spring proxy behavior.

Remember that self-invocation bypasses Spring transaction proxies. Avoid long-running transactions.
Avoid external HTTP calls inside database transactions unless there is a clear reason.

Do not assume Kafka + database operations become one atomic transaction automatically.

---

# REST API design

Controllers should normally be thin. Prefer

```text
Controller → Service → Client / Repository
```

when this separation reflects actual responsibilities. Do not create layers merely to satisfy a
diagram.

Controllers should primarily handle HTTP input, validation, mapping and HTTP response semantics.
Business logic should normally live outside controllers.

Do not expose external provider DTOs, persistence entities or internal exceptions directly through the
public REST API unless explicitly intended.

---

# External REST clients

External integrations must be isolated from application/domain models:

```text
External API → provider-specific DTO → mapping → application model
```

Do not leak Binance-specific DTOs through controllers. External client configuration must not be
hardcoded inside business logic; prefer external configuration for base URL and timeouts.

Use the HTTP client already established by the project: `RestClient` (configured in
`BinanceClientConfig`, timeouts from `BinanceProperties`). Do not introduce WebFlux only to perform
simple synchronous REST calls.

---

# Binance integration

The current Binance integration uses public market-data REST APIs (no API key), base URL
`https://data-api.binance.vision` (configured, not hardcoded).

Keep provider-specific logic isolated in the `binance` package. Relevant functionality: symbol
information, current price, 24h statistics, candles/klines.

Normalize trading symbols consistently — `btcusdt`, `BtcUsdt`, `BTCUSDT` are the same symbol
(`BTCUSDT`) unless an API contract explicitly requires otherwise. Do not hardcode supported trading
symbols. Treat Binance as the source of truth.

## Binance klines

Binance returns klines as positional arrays. Do not scatter numeric indexes throughout business
logic; keep positional mapping in the integration-level mapper (`BinanceKlineMapper`):

```text
raw Binance response → Binance kline mapper → Candle
```

Application code should not need to know that Binance represents candles as arrays.

---

# External API failure handling

Always distinguish different categories of failures. Consider separately:

* invalid input
* unknown symbol
* external API 4xx
* HTTP 429 (and Binance's 418 IP ban)
* external API 5xx
* connection failure
* timeout
* malformed provider response

Do not use `catch (Exception e)` as normal error handling. Do not silently swallow exceptions.
Preserve useful root causes when wrapping exceptions. Do not expose unnecessary provider/internal
implementation details to API clients.

Do not automatically retry every failure. Especially do not blindly retry validation errors,
deterministic 4xx responses or 429 responses without a deliberate retry strategy.

---

# Kafka

Kafka infrastructure already exists in the local Docker environment and `pom.xml`
(`spring-boot-starter-kafka`, `spring-kafka-test`). Do not remove Kafka configuration just because a
current feature does not use it. Do not introduce Kafka into a feature unless the task requires it.
Current REST functionality should remain independent from Kafka where possible.

The future streaming architecture is expected to evolve toward:

```text
Binance WebSocket → market-data ingester → Kafka → consumers / processors
```

When implementing Kafka functionality, explicitly reason about topic responsibility, message keys,
partitioning, ordering, consumer groups, offsets, duplicate processing, idempotency, retries, DLT
strategy, rebalancing, serialization, consumer lag, throughput and latency.

Never assume global ordering in Kafka; ordering exists only within a partition.

Do not introduce Kafka Streams, DLTs, retry topics, transactions or Schema Registry merely because they
are available. They must solve a concrete requirement.

---

# Performance

Always consider performance, but optimize based on expected workload and evidence. Investigate in
approximately this order:

1. algorithmic complexity
2. database calls
3. external/network calls
4. repeated work
5. concurrency and contention
6. serialization
7. allocations / GC pressure
8. micro-optimizations

Look for O(n²) behavior, repeated collection scans, repeated API/DB calls, unnecessary collection
copies, expensive work inside loops, unnecessary parsing, blocking operations, excessive object
creation in hot paths, unbounded queues, and excessive logging in high-volume paths.

Do not claim exact performance improvements without measurement. Prefer "this changes lookup
complexity from O(n) to expected O(1)" over "this makes the implementation 50% faster" unless
benchmark data supports the claim.

Do not sacrifice maintainability for tiny theoretical gains. Distinguish a real
scalability/performance problem from a micro-optimization; prefer clear code unless profiling,
expected volume or architecture indicates performance is significant.

---

# Concurrency

Assume Spring request processing is concurrent. Review mutable shared state carefully: thread safety,
compound operations, collection safety, race conditions, synchronization, lock contention.

Do not introduce synchronization without understanding the contention and lifecycle implications.

---

# Logging

Use appropriate log levels. Do not log sensitive information. Avoid logging every event at `INFO` in
high-throughput processing paths. Prefer structured, meaningful logging.

Do not log an exception and immediately rethrow it unless there is a reason; this causes duplicate
logging.

---

# Configuration and secrets

Do not hardcode environment-specific values in Java code; use application configuration.

Distinguish secrets (passwords, credentials, private API keys, access tokens) from ordinary
configuration (URLs, timeouts, topic names, batch sizes, feature flags). Do not recommend storing
normal configuration in a secret store simply because one is available. Never commit real credentials.

---

# Refactoring rules

When asked to refactor: preserve behavior, inspect callers and tests, make the smallest coherent
improvement, improve readability or structure for a concrete reason, verify after changes.

Do not turn simple code into a design-pattern exercise. Avoid unnecessary `SomethingService` /
`SomethingServiceImpl` / `SomethingStrategy` / `SomethingStrategyFactory` / `AbstractSomethingProvider`
when one focused class solves the problem cleanly. Abstractions must earn their complexity.

---

# Code review rules

When asked for code review: do not manufacture findings. Report only issues supported by code,
configuration, tests, framework semantics or observable behavior.

Prioritize: correctness → data loss → concurrency → performance → failure handling →
maintainability → tests → style.

For meaningful findings explain where the problem is, what can happen, why, when it happens, and how
to fix it. If the implementation is good, say so. Do not create comments merely to appear thorough.

---

# Tests

Every meaningful behavior change should have appropriate tests. Follow existing repository test
conventions (see Repository facts). Prefer focused tests; use integration tests when integration
behavior itself matters. Do not use `@SpringBootTest` when a smaller test is sufficient.

External APIs must not be called from automated tests. Mock the Binance API with
`MockRestServiceServer`, which is already used in the repository; do not add WireMock or MockWebServer.

Tests should focus on behavior rather than implementation details. Important cases: happy path,
boundary values, invalid input, provider errors, empty responses, duplicate input/events where
relevant, failure paths, mapping.

---

# Build and verification

Use the Maven Wrapper, not a system-installed Maven:

```bash
./mvnw -q test                          # all tests
./mvnw -q test -Dtest=ClassNameTest     # focused
./mvnw -q -DskipTests package           # compile/package check
```

After implementation:

1. run focused tests
2. run the relevant tests
3. run compile/build verification
4. inspect the final diff (`git diff`)

Do not claim a test passed or a build succeeded unless it was actually executed. If verification
cannot be performed, state exactly why. If a failure is unrelated to the change, separate it clearly
from failures caused by the implementation.

---

# Dependencies

Do not add a new dependency if the repository already provides equivalent functionality. Before adding
one: inspect existing dependencies, determine whether it is necessary, prefer Spring/JDK capabilities
where reasonable, and explain why the new dependency is justified. Avoid bringing large frameworks into
the project for small problems.

---

# Comments and documentation

Prefer self-explanatory code. Comments should explain why something non-obvious is required, external
constraints (e.g. Binance error codes, kline array layout), subtle correctness requirements, and
important framework behavior (e.g. why a method must be public for a proxy). Do not comment what the
code plainly says. Update `README.md` when endpoints, configuration or the project layout change.
