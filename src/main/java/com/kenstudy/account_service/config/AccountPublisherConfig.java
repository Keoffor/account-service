package com.kenstudy.account_service.config;

import com.kenstudy.event.AccountEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.function.Supplier;

@Configuration
public class AccountPublisherConfig {

    @Bean
    public Sinks.Many<AccountEvent> accountSinks(){

        return Sinks.many().multicast().onBackpressureBuffer();
    }

    @Bean
    public Supplier<Flux<AccountEvent>> accountSupplier(Sinks.Many<AccountEvent> sinks){

        return sinks::asFlux;
    }
}
