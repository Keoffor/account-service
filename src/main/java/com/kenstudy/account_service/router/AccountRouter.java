package com.kenstudy.account_service.router;

import com.kenstudy.account_service.handler.CustomerAccountHandler;
import com.kenstudy.account_service.handler.TransactionHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

@Configuration
public class AccountRouter {
    @Bean
    public RouterFunction<ServerResponse> route(CustomerAccountHandler accountHandler,
                                                TransactionHandler transactHandler) {
        return RouterFunctions.route()
                .path("/v1/account", builder -> builder
                        .POST("/{id}/add",
                                RequestPredicates.contentType(MediaType.APPLICATION_JSON),
                                accountHandler::createAccount)

                        .GET("/{accountId}",
                                RequestPredicates.accept(MediaType.APPLICATION_JSON),
                                accountHandler::getAccountDetails)

                        .GET("/customer-acct-details/{accountId}",
                                RequestPredicates.accept(MediaType.APPLICATION_JSON),
                                accountHandler::getCustomerAndAcctDetails)

                        .POST("/fund-transfer",
                                RequestPredicates.contentType(MediaType.APPLICATION_JSON),
                                transactHandler::processTransferRequest)
                )
                .build();
    }


}
