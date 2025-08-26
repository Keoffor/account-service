package com.kenstudy.account_service.saga;

import com.kenstudy.account_service.config.AccountPublisherConfig;
import com.kenstudy.account_service.exception.BalanceNotFound;
import com.kenstudy.event.AccountEvent;
import com.kenstudy.event.TransactEvent;
import com.kenstudy.payment.PaymentRequestDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
@Configuration
public class AccountConsumer {
    private final AccountHandler handler;

    private final StreamBridge streamBridge;

    @Autowired
    public AccountConsumer(AccountHandler handler, StreamBridge streamBridge) {
        this.handler = handler;
        this.streamBridge = streamBridge;
    }


    @Bean
    public Function<Flux<TransactEvent>, Flux<AccountEvent>> processTransferFund() {
        return transactEvents -> transactEvents.flatMap(event ->
                handler.processTransfer(event)
                        .flatMap(accountEvent -> {
                            if (accountEvent.isError()) {
                                // Failure path: send directly to failure topic
                                streamBridge.send("accountEventFailure-out-0", accountEvent);
                                return Mono.empty();
                            } else {
                                // Success path: let it go to success topic
                                return Mono.just(accountEvent);
                            }
                        })
                        .onErrorResume(ex -> {
                            log.error("Unexpected error in processTransfer: {}", ex.getMessage(), ex);
                            // Create failure event for the unexpected error
                            PaymentRequestDTO dto = handler.mapToAccountDto(event.getTransRequestDTO());
                            return handler.handleFailureEvent(new AccountEvent(), dto, ex.getMessage())
                                    .doOnNext(failure -> streamBridge
                                            .send("accountEventFailure-out-0", failure))
                                    .then(Mono.empty());
                        })
        );
    }


}
