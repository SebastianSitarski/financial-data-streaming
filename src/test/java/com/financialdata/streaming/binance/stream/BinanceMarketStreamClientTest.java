package com.financialdata.streaming.binance.stream;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;

import com.financialdata.streaming.binance.BinanceProperties;
import com.financialdata.streaming.stream.MarketDataProperties;
import com.financialdata.streaming.stream.MarketUpdate;
import com.financialdata.streaming.stream.MarketUpdatePublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.WebSocketClient;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BinanceMarketStreamClientTest {

    private static final String STREAM_URL =
            "wss://binance.test/stream?streams=btcusdt@ticker/ethusdt@ticker";
    private static final Instant START = Instant.parse("2026-09-20T10:00:00Z");
    private static final String TICKER = """
            {"stream":"btcusdt@ticker","data":{"e":"24hrTicker","E":1789812000123,"s":"BTCUSDT","p":"1","P":"0.5",
             "c":"116532.42","h":"117100","l":"114220.5","v":"18452.43"}}
            """;

    @Mock
    private WebSocketClient webSocketClient;
    @Mock
    private MarketUpdatePublisher publisher;
    @Mock
    private TaskScheduler scheduler;
    @Mock
    private WebSocketSession session;

    private final MutableClock clock = new MutableClock(START);
    private BinanceMarketStreamClient client;

    @BeforeEach
    void setUp() {
        BinanceProperties binance = new BinanceProperties("https://binance.test", Duration.ofSeconds(5),
                Duration.ofSeconds(10), "wss://binance.test",
                new BinanceProperties.Stream(true, Duration.ofSeconds(1), Duration.ofSeconds(8), Duration.ofSeconds(30)));
        MarketDataProperties marketData = new MarketDataProperties(List.of("BTCUSDT", "ETHUSDT"),
                "market-update-processor", new MarketDataProperties.Topic("crypto.market-updates", 3, (short) 1, Duration.ofHours(1)));
        client = new BinanceMarketStreamClient(webSocketClient, new BinanceTickerMapper(JsonMapper.builder().build()),
                publisher, binance, marketData, scheduler, clock);
    }

    @Test
    void startOpensOneCombinedStreamForAllConfiguredSymbols() {
        connectSuccessfully();

        verify(webSocketClient, times(1)).execute(any(WebSocketHandler.class), eq(STREAM_URL));
        assertThat(client.isRunning()).isTrue();
        verifyNoInteractions(scheduler);
    }

    @Test
    void tickerMessagesArePublishedAsMarketUpdates() throws Exception {
        WebSocketHandler handler = connectSuccessfully();

        handler.handleMessage(session, new TextMessage(TICKER));

        ArgumentCaptor<MarketUpdate> published = ArgumentCaptor.forClass(MarketUpdate.class);
        verify(publisher).publish(published.capture());
        assertThat(published.getValue().symbol()).isEqualTo("BTCUSDT");
        assertThat(published.getValue().lastPrice()).isEqualByComparingTo(new BigDecimal("116532.42"));
        assertThat(published.getValue().eventTime()).isEqualTo(Instant.ofEpochMilli(1789812000123L));
    }

    @Test
    void unreadableMessageIsSkippedAndStreamStaysUp() throws Exception {
        WebSocketHandler handler = connectSuccessfully();

        handler.handleMessage(session, new TextMessage("not json"));
        handler.handleMessage(session, new TextMessage(TICKER));

        verify(publisher, times(1)).publish(any());
        verifyNoInteractions(scheduler);
    }

    @Test
    void connectionCloseSchedulesOneReconnectAfterInitialBackoff() throws Exception {
        WebSocketHandler handler = connectSuccessfully();
        stubScheduler();

        handler.afterConnectionClosed(session, CloseStatus.NORMAL);
        handler.afterConnectionClosed(session, CloseStatus.NORMAL);

        verify(scheduler, times(1)).schedule(any(Runnable.class), eq(START.plusSeconds(1)));
    }

    @Test
    void scheduledReconnectOpensTheStreamAgainWithTheSameSubscriptions() throws Exception {
        WebSocketHandler handler = connectSuccessfully();
        ArgumentCaptor<Runnable> reconnect = stubScheduler();

        handler.afterConnectionClosed(session, CloseStatus.SESSION_NOT_RELIABLE);
        clock.advance(Duration.ofSeconds(1));
        reconnect.getValue().run();

        verify(webSocketClient, times(2)).execute(any(WebSocketHandler.class), eq(STREAM_URL));
    }

    @Test
    void repeatedUnstableConnectionsBackOffExponentiallyUpToTheMaximum() throws Exception {
        WebSocketHandler handler = connectSuccessfully();
        ArgumentCaptor<Runnable> reconnect = stubScheduler();
        ArgumentCaptor<Instant> at = ArgumentCaptor.forClass(Instant.class);

        for (int i = 0; i < 5; i++) {
            handler.afterConnectionClosed(session, CloseStatus.SERVER_ERROR);
            reconnect.getValue().run();
            handler = latestHandler();
            handler.afterConnectionEstablished(session);
        }

        verify(scheduler, times(5)).schedule(any(Runnable.class), at.capture());
        assertThat(at.getAllValues()).containsExactly(
                START.plusSeconds(1), START.plusSeconds(2), START.plusSeconds(4),
                START.plusSeconds(8), START.plusSeconds(8));
    }

    @Test
    void backoffResetsAfterAStableConnection() throws Exception {
        WebSocketHandler handler = connectSuccessfully();
        ArgumentCaptor<Runnable> reconnect = stubScheduler();
        ArgumentCaptor<Instant> at = ArgumentCaptor.forClass(Instant.class);

        handler.afterConnectionClosed(session, CloseStatus.SERVER_ERROR);       // attempt 0 -> 1s
        reconnect.getValue().run();
        handler = latestHandler();
        handler.afterConnectionEstablished(session);
        handler.afterConnectionClosed(session, CloseStatus.SERVER_ERROR);       // attempt 1 -> 2s
        reconnect.getValue().run();
        handler = latestHandler();
        handler.afterConnectionEstablished(session);
        clock.advance(Duration.ofSeconds(30));                                  // stable for stable-after
        handler.afterConnectionClosed(session, CloseStatus.SERVER_ERROR);       // reset -> 1s again

        verify(scheduler, times(3)).schedule(any(Runnable.class), at.capture());
        assertThat(at.getAllValues()).containsExactly(
                START.plusSeconds(1), START.plusSeconds(2), START.plusSeconds(30).plusSeconds(1));
    }

    @Test
    void closeBeforeConnectFutureCompletesStillReconnects() throws Exception {
        // Tomcat reports onOpen (and possibly onClose) before StandardWebSocketClient completes its future
        CompletableFuture<WebSocketSession> neverCompleted = new CompletableFuture<>();
        when(webSocketClient.execute(any(WebSocketHandler.class), anyString())).thenAnswer(invocation -> {
            WebSocketHandler handler = invocation.getArgument(0);
            handler.afterConnectionEstablished(session);
            return neverCompleted;
        });
        ArgumentCaptor<Runnable> reconnect = stubScheduler();
        client.start();
        WebSocketHandler handler = latestHandler();

        handler.afterConnectionClosed(session, CloseStatus.SERVER_ERROR);
        reconnect.getValue().run();

        verify(webSocketClient, times(2)).execute(any(WebSocketHandler.class), eq(STREAM_URL));
    }

    @Test
    void failedConnectionAttemptSchedulesReconnect() {
        stubScheduler();
        CompletableFuture<WebSocketSession> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("connection refused"));
        when(webSocketClient.execute(any(WebSocketHandler.class), anyString())).thenReturn(failed);

        client.start();

        verify(scheduler).schedule(any(Runnable.class), eq(START.plusSeconds(1)));
    }

    @Test
    void stopClosesSessionAndDoesNotReconnect() throws Exception {
        WebSocketHandler handler = connectSuccessfully();
        when(session.isOpen()).thenReturn(true);

        client.stop();
        handler.afterConnectionClosed(session, CloseStatus.GOING_AWAY);

        verify(session).close(CloseStatus.GOING_AWAY);
        assertThat(client.isRunning()).isFalse();
        verifyNoInteractions(scheduler);
    }

    @Test
    void stopCancelsAPendingReconnect() throws Exception {
        WebSocketHandler handler = connectSuccessfully();
        stubScheduler();

        handler.afterConnectionClosed(session, CloseStatus.SERVER_ERROR);
        client.stop();

        verify(pendingFuture).cancel(false);
    }

    // -- helpers -------------------------------------------------------------------------------------------

    @SuppressWarnings("rawtypes")
    private final ScheduledFuture pendingFuture = mock(ScheduledFuture.class);

    /** Mimics the container: the handler is told about the open session before the future completes. */
    private WebSocketHandler connectSuccessfully() {
        when(webSocketClient.execute(any(WebSocketHandler.class), anyString())).thenAnswer(invocation -> {
            WebSocketHandler handler = invocation.getArgument(0);
            handler.afterConnectionEstablished(session);
            return CompletableFuture.completedFuture(session);
        });
        client.start();
        return latestHandler();
    }

    private WebSocketHandler latestHandler() {
        ArgumentCaptor<WebSocketHandler> handler = ArgumentCaptor.forClass(WebSocketHandler.class);
        verify(webSocketClient, org.mockito.Mockito.atLeastOnce()).execute(handler.capture(), anyString());
        return handler.getValue();
    }

    @SuppressWarnings("unchecked")
    private ArgumentCaptor<Runnable> stubScheduler() {
        ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
        when(scheduler.schedule(task.capture(), any(Instant.class))).thenReturn(pendingFuture);
        return task;
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
