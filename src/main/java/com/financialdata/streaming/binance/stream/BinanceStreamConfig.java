package com.financialdata.streaming.binance.stream;

import jakarta.websocket.ContainerProvider;
import jakarta.websocket.WebSocketContainer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.client.WebSocketClient;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;

@Configuration(proxyBeanMethods = false)
class BinanceStreamConfig {

    /** Ticker messages are well under 1 KB; the raised limit only guards against a larger future payload. */
    private static final int MAX_TEXT_MESSAGE_BYTES = 64 * 1024;

    /**
     * Jakarta WebSocket client backed by the embedded Tomcat container, which already answers Binance's
     * periodic pings with pongs, so no manual keep-alive is needed.
     */
    @Bean
    WebSocketClient binanceWebSocketClient() {
        WebSocketContainer container = ContainerProvider.getWebSocketContainer();
        container.setDefaultMaxTextMessageBufferSize(MAX_TEXT_MESSAGE_BYTES);
        return new StandardWebSocketClient(container);
    }
}
