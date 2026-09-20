package com.financialdata.streaming.stream;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the shared market-update topic; Boot's auto-configured {@code KafkaAdmin} creates it on
 * startup when the broker is reachable and leaves it untouched otherwise.
 */
@Configuration(proxyBeanMethods = false)
class MarketUpdateTopicConfig {

    @Bean
    NewTopic marketUpdatesTopic(MarketDataProperties properties) {
        MarketDataProperties.Topic topic = properties.topic();
        return TopicBuilder.name(topic.name())
                .partitions(topic.partitions())
                .replicas(topic.replicationFactor())
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(topic.retention().toMillis()))
                .build();
    }
}
