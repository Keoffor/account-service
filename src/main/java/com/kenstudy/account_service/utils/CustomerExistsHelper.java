package com.kenstudy.account_service.utils;

import com.kenstudy.account_service.repository.AccountRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@Slf4j
public class CustomerExistsHelper {
    private final AccountRepository accountRepository;

    @Autowired
    public CustomerExistsHelper(AccountRepository accountRepository) {
        this.accountRepository = accountRepository;
    }

    public Mono<Boolean> checkCustomerExists(Integer userId, Integer accountId) {
        return Mono.justOrEmpty(accountId)
                .switchIfEmpty(Mono.error(new RuntimeException("Account Id does not exist")))
                .flatMap(accountRepository::findById)
                .flatMap(cusAcct ->
                        accountRepository.existsByCustomersId(userId)
                                .map(exists -> {
                                    boolean result = cusAcct.getCustomersId().equals(userId) && exists;
                                    log.debug("Customer check for userId={} result={}", userId, result);
                                    return result;
                                })
                );
    }


}
