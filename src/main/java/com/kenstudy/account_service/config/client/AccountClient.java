package com.kenstudy.account_service.config.client;

import com.kenstudy.account_service.exception.AccountNotFoundException;
import com.kenstudy.account_service.exception.ErrorMessageResponse;
import com.kenstudy.account_service.exception.ResourceNotFoundException;
import com.kenstudy.customer.CustomerResponseDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Component
public class AccountClient {

    @Autowired
    WebClient webClient;

    String URL_USER_HOST = "http://localhost:4000";
    public Mono<CustomerResponseDTO> getCustomerDetails(Integer userId) {
        return webClient.get()
                        .uri(URL_USER_HOST + "/v1/bank/{id}", userId)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .onStatus(HttpStatusCode::is4xxClientError, clientResponse ->
                        clientResponse.bodyToMono(ErrorMessageResponse.class)
                            .flatMap(body -> Mono.error(
                                new AccountNotFoundException(
                                    "User service client error: " + body.getMessage()))))
                .onStatus(HttpStatusCode::is5xxServerError, clientResponse ->
                        clientResponse.bodyToMono(ErrorMessageResponse.class)
                            .flatMap(body -> Mono.error(
                                new ResourceNotFoundException(
                                    "User service server error: " + body.getMessage()))))
                .bodyToMono(CustomerResponseDTO.class);
    }
}
