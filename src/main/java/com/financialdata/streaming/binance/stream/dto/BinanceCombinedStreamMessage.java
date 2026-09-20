package com.financialdata.streaming.binance.stream.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Envelope used by Binance combined streams ({@code /stream?streams=a@ticker/b@ticker}):
 * {@code {"stream":"btcusdt@ticker","data":{...}}}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BinanceCombinedStreamMessage(String stream, BinanceTickerEvent data) {
}
