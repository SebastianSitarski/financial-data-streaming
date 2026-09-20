package com.financialdata.streaming.binance.stream;

import java.math.BigDecimal;
import java.time.Instant;

import com.financialdata.streaming.stream.MarketUpdate;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class BinanceTickerMapperTest {

    private static final String TICKER = """
            {"stream":"btcusdt@ticker","data":{"e":"24hrTicker","E":1789812000123,"s":"BTCUSDT",
             "p":"1243.54000000","P":"1.080","w":"115900.12","x":"115288.88","c":"116532.42000000","Q":"0.00500000",
             "b":"116532.41000000","B":"1.2","a":"116532.42000000","A":"0.8","o":"115288.88000000",
             "h":"117100.00000000","l":"114220.50000000","v":"18452.43000000","q":"2138000000.12",
             "O":1789725600123,"C":1789812000123,"F":100,"L":200,"n":101}}
            """;

    private final BinanceTickerMapper mapper = new BinanceTickerMapper(JsonMapper.builder().build());

    @Test
    void mapsCombinedStreamTickerToMarketUpdate() {
        MarketUpdate update = mapper.toMarketUpdate(TICKER);

        assertThat(update).isEqualTo(new MarketUpdate(
                "BTCUSDT",
                Instant.ofEpochMilli(1789812000123L),
                new BigDecimal("116532.42000000"),
                new BigDecimal("1243.54000000"),
                new BigDecimal("1.080"),
                new BigDecimal("117100.00000000"),
                new BigDecimal("114220.50000000"),
                new BigDecimal("18452.43000000")));
    }

    @Test
    void preservesDecimalScaleInsteadOfGoingThroughDouble() {
        MarketUpdate update = mapper.toMarketUpdate(TICKER);

        assertThat(update.lastPrice().scale()).isEqualTo(8);
        assertThat(update.priceChangePercent()).isEqualTo(new BigDecimal("1.080"));
    }

    @Test
    void rejectsMalformedJson() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> mapper.toMarketUpdate("{\"stream\":\"btcusdt@ticker\",\"data\":"))
                .withMessage("Malformed Binance stream message");
    }

    @Test
    void rejectsMessageWithoutDataPayload() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> mapper.toMarketUpdate("{\"result\":null,\"id\":1}"))
                .withMessage("Binance stream message has no data payload");
    }

    @Test
    void rejectsTickerWithMissingRequiredFieldNamingIt() {
        String withoutLastPrice = TICKER.replace("\"c\":\"116532.42000000\",", "");

        assertThatIllegalArgumentException()
                .isThrownBy(() -> mapper.toMarketUpdate(withoutLastPrice))
                .withMessage("Binance ticker field 'c' is missing");
    }
}
