package com.financialdata.streaming;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class FinancialDataStreamingApplication {

    public static void main(String[] args) {
        SpringApplication.run(FinancialDataStreamingApplication.class, args);
    }
}
