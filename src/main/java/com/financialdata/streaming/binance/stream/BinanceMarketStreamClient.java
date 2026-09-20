package com.financialdata.streaming.binance.stream;

import java.io.IOException;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.financialdata.streaming.binance.BinanceProperties;
import com.financialdata.streaming.stream.MarketDataProperties;
import com.financialdata.streaming.stream.MarketUpdate;
import com.financialdata.streaming.stream.MarketUpdatePublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.WebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/**
 * Keeps one combined Binance ticker stream open for the configured symbols and forwards every ticker as
 * a {@link MarketUpdate} to Kafka.
 *
 * <p>Lifecycle: the connection is opened asynchronously in {@link #start()} (after the context is
 * refreshed, in the last phase so Kafka listener containers are already running) and closed in
 * {@link #stop()}. Any close or connect failure while running schedules exactly one reconnect with
 * exponential back-off; the back-off resets after the connection has been up for
 * {@code binance.stream.stable-after}. Binance itself drops connections after 24h, so reconnecting is
 * normal operation, not an error path.
 */
@Component
@ConditionalOnProperty(name = "binance.stream.enabled", havingValue = "true", matchIfMissing = true)
public class BinanceMarketStreamClient implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(BinanceMarketStreamClient.class);

    private final WebSocketClient webSocketClient;
    private final BinanceTickerMapper mapper;
    private final MarketUpdatePublisher publisher;
    private final TaskScheduler scheduler;
    private final Clock clock;
    private final BinanceProperties.Stream settings;
    private final URI streamUri;
    private final int symbolCount;
    private final boolean ownsScheduler;

    private volatile boolean running;
    private final AtomicBoolean connecting = new AtomicBoolean();
    private final AtomicBoolean reconnectPending = new AtomicBoolean();
    private final AtomicInteger failedAttempts = new AtomicInteger();
    private final AtomicReference<WebSocketSession> session = new AtomicReference<>();
    private final AtomicReference<ScheduledFuture<?>> scheduledReconnect = new AtomicReference<>();
    private volatile Instant connectedAt;

    @Autowired
    public BinanceMarketStreamClient(WebSocketClient binanceWebSocketClient, BinanceTickerMapper mapper,
            MarketUpdatePublisher publisher, BinanceProperties binanceProperties,
            MarketDataProperties marketDataProperties) {
        this(binanceWebSocketClient, mapper, publisher, binanceProperties, marketDataProperties,
                newScheduler(), Clock.systemUTC(), true);
    }

    BinanceMarketStreamClient(WebSocketClient binanceWebSocketClient, BinanceTickerMapper mapper,
            MarketUpdatePublisher publisher, BinanceProperties binanceProperties,
            MarketDataProperties marketDataProperties, TaskScheduler scheduler, Clock clock) {
        this(binanceWebSocketClient, mapper, publisher, binanceProperties, marketDataProperties, scheduler, clock,
                false);
    }

    private BinanceMarketStreamClient(WebSocketClient binanceWebSocketClient, BinanceTickerMapper mapper,
            MarketUpdatePublisher publisher, BinanceProperties binanceProperties,
            MarketDataProperties marketDataProperties, TaskScheduler scheduler, Clock clock, boolean ownsScheduler) {
        this.webSocketClient = binanceWebSocketClient;
        this.mapper = mapper;
        this.publisher = publisher;
        this.scheduler = scheduler;
        this.clock = clock;
        this.ownsScheduler = ownsScheduler;
        this.settings = binanceProperties.stream();
        List<String> symbols = marketDataProperties.symbols();
        this.symbolCount = symbols.size();
        this.streamUri = BinanceStreamUris.combinedTickerStream(binanceProperties.wsUrl(), symbols);
    }

    private static TaskScheduler newScheduler() {
        ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.setPoolSize(1);
        taskScheduler.setThreadNamePrefix("binance-stream-");
        taskScheduler.initialize();
        return taskScheduler;
    }

    @Override
    public void start() {
        running = true;
        connect();
    }

    @Override
    public void stop() {
        running = false;
        ScheduledFuture<?> pending = scheduledReconnect.getAndSet(null);
        if (pending != null) {
            pending.cancel(false);
        }
        closeSession(CloseStatus.GOING_AWAY);
        if (ownsScheduler && scheduler instanceof ThreadPoolTaskScheduler owned) {
            owned.shutdown();
        }
        log.info("Binance market stream stopped");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void connect() {
        if (!running || !connecting.compareAndSet(false, true)) {
            return;
        }
        WebSocketSession current = session.get();
        if (current != null && current.isOpen()) {
            connecting.set(false);
            return;
        }
        log.info("Connecting to Binance market stream for {} symbols: {}", symbolCount, streamUri);
        webSocketClient.execute(new TickerHandler(), streamUri.toString()).whenComplete((newSession, failure) -> {
            connecting.set(false);
            if (failure != null) {
                log.warn("Binance market stream connection failed: {}", failure.toString());
                scheduleReconnect();
            }
        });
    }

    private void scheduleReconnect() {
        if (!running || !reconnectPending.compareAndSet(false, true)) {
            return;
        }
        Duration delay = backoff(failedAttempts.getAndIncrement());
        log.info("Reconnecting to Binance market stream in {}", delay);
        scheduledReconnect.set(scheduler.schedule(() -> {
            scheduledReconnect.set(null);
            reconnectPending.set(false);
            connect();
        }, clock.instant().plus(delay)));
    }

    /** {@code initial * 2^attempt}, capped at {@code maxBackoff}; the shift is bounded so it cannot overflow. */
    private Duration backoff(int attempt) {
        Duration initial = settings.initialBackoff();
        Duration max = settings.maxBackoff();
        if (attempt >= 30) {
            return max;
        }
        Duration candidate = initial.multipliedBy(1L << attempt);
        return candidate.compareTo(max) > 0 ? max : candidate;
    }

    private void closeSession(CloseStatus status) {
        WebSocketSession current = session.getAndSet(null);
        if (current != null && current.isOpen()) {
            try {
                current.close(status);
            }
            catch (IOException ex) {
                log.debug("Closing Binance market stream session failed", ex);
            }
        }
    }

    private final class TickerHandler extends TextWebSocketHandler {

        @Override
        public void afterConnectionEstablished(WebSocketSession newSession) {
            session.set(newSession);
            connectedAt = clock.instant();
            // the container reports the open session before execute()'s future completes; from here on the
            // open session is what blocks duplicate connects, so an immediate close may already reconnect
            connecting.set(false);
            log.info("Binance market stream connected ({} symbols)", symbolCount);
        }

        @Override
        protected void handleTextMessage(WebSocketSession currentSession, TextMessage message) {
            MarketUpdate update;
            try {
                update = mapper.toMarketUpdate(message.getPayload());
            }
            catch (IllegalArgumentException ex) {
                // one bad message must not cost us the stream; the next ticker supersedes it anyway
                log.warn("Skipping unreadable Binance ticker message: {}", ex.getMessage());
                return;
            }
            log.debug("Received ticker {}", update);
            publisher.publish(update);
        }

        @Override
        public void handleTransportError(WebSocketSession currentSession, Throwable exception) {
            // the container closes the session afterwards, which triggers the reconnect
            log.warn("Binance market stream transport error: {}", exception.toString());
        }

        @Override
        public void afterConnectionClosed(WebSocketSession closedSession, CloseStatus status) {
            session.compareAndSet(closedSession, null);
            Instant openedAt = connectedAt;
            boolean wasStable = openedAt != null
                    && !Duration.between(openedAt, clock.instant()).minus(settings.stableAfter()).isNegative();
            if (wasStable) {
                failedAttempts.set(0);
            }
            if (!running) {
                return;
            }
            log.warn("Binance market stream disconnected: {}", status);
            scheduleReconnect();
        }
    }
}
