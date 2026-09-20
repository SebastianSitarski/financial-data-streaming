package com.financialdata.streaming.stream;

import java.time.Duration;
import java.util.List;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MarketUpdateTopicConfigTest {

    @Test
    void topicCarriesConfiguredPartitionsReplicationAndShortRetention() {
        MarketDataProperties properties = new MarketDataProperties(List.of("BTCUSDT"), "market-update-processor",
                new MarketDataProperties.Topic("crypto.market-updates", 3, (short) 1, Duration.ofHours(1)));

        NewTopic topic = new MarketUpdateTopicConfig().marketUpdatesTopic(properties);

        assertThat(topic.name()).isEqualTo("crypto.market-updates");
        assertThat(topic.numPartitions()).isEqualTo(3);
        assertThat(topic.replicationFactor()).isEqualTo((short) 1);
        assertThat(topic.configs()).containsEntry(TopicConfig.RETENTION_MS_CONFIG, "3600000");
    }
}
