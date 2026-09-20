package com.financialdata.streaming.binance;

import org.springframework.boot.http.client.ClientHttpRequestFactoryBuilder;
import org.springframework.boot.http.client.HttpClientSettings;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

@Configuration(proxyBeanMethods = false)
class BinanceClientConfig {

    /**
     * Starts from Boot's auto-configured factory builder and {@link HttpClientSettings} (built from
     * {@code spring.http.clients.*}: SSL bundle, redirects, ...) so global settings still apply; only the
     * timeouts are overridden per Binance client.
     */
    @Bean
    RestClient binanceRestClient(RestClient.Builder builder, ClientHttpRequestFactoryBuilder<?> factoryBuilder,
            HttpClientSettings globalSettings, BinanceProperties properties) {
        HttpClientSettings settings = globalSettings
                .withTimeouts(properties.connectTimeout(), properties.readTimeout());
        return builder
                .baseUrl(properties.baseUrl())
                .requestFactory(factoryBuilder.build(settings))
                .build();
    }
}
