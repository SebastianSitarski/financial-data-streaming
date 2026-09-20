package com.financialdata.streaming;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

// the Binance WebSocket client is disabled so the context test never opens a network connection
@SpringBootTest(properties = "binance.stream.enabled=false")
@EmbeddedKafka(partitions = 1, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class FinancialDataStreamingApplicationTests {

    @Test
    void contextLoads() {
    }
}
