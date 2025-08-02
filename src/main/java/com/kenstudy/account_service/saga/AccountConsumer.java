package com.kenstudy.account_service.saga;

import com.kenstudy.event.AccountEvent;
import com.kenstudy.event.TransactEvent;
import com.kenstudy.event.status.TransStatus;
import com.kenstudy.payment.PaymentRequestDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.BiConsumer;
import java.util.function.Function;

@Configuration
public class AccountConsumer {
    private final AccountHandler handler;

    @Autowired
    public AccountConsumer(AccountHandler handler) {
        this.handler = handler;
    }

    @Bean
    public Function<Flux<TransactEvent>, Flux<AccountEvent>> transferFund() {
        return transactEvent -> transactEvent.flatMap(this::processTransferFund);
    }

    private Mono<AccountEvent> processTransferFund(TransactEvent transactEvent) {
        if (TransStatus.TRANSACTION_INITIATED.equals(transactEvent.getTransStatus())
                && !transactEvent.isEventClosed()) {
            return this.handler.processTransfer(transactEvent);
        } else {
            return Mono.fromRunnable(() -> this.handler.compensateTransact(transactEvent));
        }
    }

    @Bean
    public BiConsumer<PaymentRequestDTO, String> updateTransactAcct() {
        return handler::recordFailedTransact;
    }


}
