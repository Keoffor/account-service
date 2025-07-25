package com.kenstudy.account_service.handler;

import com.kenstudy.account_service.service.AccountService;
import com.kenstudy.payment.PaymentRequestDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.util.Map;

@Component
public class TransactionHandler {
    private final AccountService accountService;

    @Autowired
    public TransactionHandler(AccountService accountService) {
        this.accountService = accountService;
    }
    public Mono<ServerResponse> processTransferRequest(ServerRequest request) {
        return request.bodyToMono(PaymentRequestDTO.class)
                .flatMap(paymentRequestDTO ->
                        accountService.processTransferFundRequest(paymentRequestDTO)
                                .flatMap(transfer ->
                                        ServerResponse.status(HttpStatus.CREATED)
                                                .contentType(MediaType.APPLICATION_JSON)
                                                .bodyValue(transfer)
                                )
                );
    }

}
