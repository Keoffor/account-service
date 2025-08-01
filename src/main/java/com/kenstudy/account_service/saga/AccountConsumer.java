package com.kenstudy.account_service.saga;

import com.kenstudy.event.AccountEvent;
import com.kenstudy.event.TransactEvent;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;
import java.util.function.Function;

@Configuration
public class AccountConsumer {
    private final AccountHandler handler;

    @Autowired
    public AccountConsumer(AccountHandler handler) {
        this.handler = handler;
    }

    @Bean
    public Function<Flux<TransactEvent>,Flux<AccountEvent>> transferFund(){
    return transactEvent -> transactEvent.flatMap(this::processTransferFund);
    }

    private Mono<AccountEvent> processTransferFund(TransactEvent transactEvent) {
        return this.handler.processTransfer(transactEvent);
    }


    @Bean
    public Consumer<AccountEvent> consumeAccountEvent(){
        //listen to customer topic
        //check request created
        //if transfer request created proceed to initiate transaction
        //if transfer request status is failed, update transaction as failed.
        return handler::updateTransactAcct;
    }

}
