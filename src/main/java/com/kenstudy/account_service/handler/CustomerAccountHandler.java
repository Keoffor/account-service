package com.kenstudy.account_service.handler;

import com.kenstudy.account_service.service.AccountService;
import com.kenstudy.account_service.service.customImpl.AccountDetailsHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.util.Map;

@Component
public class CustomerAccountHandler {

    private final AccountService accountService;
    private final AccountDetailsHelper acctDetailsHelperService;

    @Autowired
    public CustomerAccountHandler(AccountService accountService, AccountDetailsHelper acctDetailsHelperService) {
        this.accountService = accountService;
        this.acctDetailsHelperService = acctDetailsHelperService;
    }




    public Mono<ServerResponse> createAccount(ServerRequest serverRequest) {
        String userId = serverRequest.pathVariable("id");

        return accountService.createAccount(userId)
                .flatMap(accountBalanceDTO ->
                        ServerResponse.status(HttpStatus.CREATED)
                                .contentType(MediaType.APPLICATION_JSON)
                                .bodyValue(accountBalanceDTO)
                );
    }

    public Mono<ServerResponse> getAccountDetails(ServerRequest serverRequest) {
        Integer accountId = Integer.parseInt(serverRequest.pathVariable("accountId"));

        return accountService.getAccountDetails(accountId)
                .flatMap(accountBalanceDTO ->
                        ServerResponse.status(HttpStatus.OK)
                                .contentType(MediaType.APPLICATION_JSON)
                                .bodyValue(accountBalanceDTO)
                );
    }

    public Mono<ServerResponse> getCustomerAndAcctDetails(ServerRequest request){
        Integer accountId = Integer.parseInt(request.pathVariable("accountId"));
        return acctDetailsHelperService.getCustomerAndAcctDetails(accountId)
                .flatMap(cusAcct ->
                        ServerResponse.status(HttpStatus.OK)
                                .contentType(MediaType.APPLICATION_JSON)
                                .bodyValue(cusAcct));

    }



}
