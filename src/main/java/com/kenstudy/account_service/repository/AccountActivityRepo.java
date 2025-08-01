package com.kenstudy.account_service.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface AccountActivityRepo extends ReactiveCrudRepository<AccountActivityRepo, Integer> {


    <T> Mono<T> saveAll(Flux<T> concat);
}
