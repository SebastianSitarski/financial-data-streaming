package com.financialdata.streaming.binance.stream;

import java.time.Instant;

import com.financialdata.streaming.binance.stream.dto.BinanceCombinedStreamMessage;
import com.financialdata.streaming.binance.stream.dto.BinanceTickerEvent;
import com.financialdata.streaming.stream.MarketUpdate;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Turns a raw combined-stream ticker message into an application {@link MarketUpdate}. JSON parsing and
 * Binance field names stay here; a message that cannot be mapped is reported as
 * {@link IllegalArgumentException} so the stream client can skip it and keep the connection alive.
 */
@Component
public class BinanceTickerMapper {

    private final JsonMapper jsonMapper;

    public BinanceTickerMapper(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    public MarketUpdate toMarketUpdate(String combinedStreamJson) {
        BinanceCombinedStreamMessage message;
        try {
            message = jsonMapper.readValue(combinedStreamJson, BinanceCombinedStreamMessage.class);
        }
        catch (JacksonException ex) {
            throw new IllegalArgumentException("Malformed Binance stream message", ex);
        }
        if (message == null || message.data() == null) {
            throw new IllegalArgumentException("Binance stream message has no data payload");
        }
        return toMarketUpdate(message.data());
    }

    MarketUpdate toMarketUpdate(BinanceTickerEvent event) {
        return new MarketUpdate(
                required(event.symbol(), "s"),
                Instant.ofEpochMilli(required(event.eventTime(), "E")),
                required(event.lastPrice(), "c"),
                required(event.priceChange(), "p"),
                required(event.priceChangePercent(), "P"),
                required(event.highPrice(), "h"),
                required(event.lowPrice(), "l"),
                required(event.volume(), "v"));
    }

    private static <T> T required(T value, String binanceField) {
        if (value == null) {
            throw new IllegalArgumentException("Binance ticker field '" + binanceField + "' is missing");
        }
        return value;
    }
}
