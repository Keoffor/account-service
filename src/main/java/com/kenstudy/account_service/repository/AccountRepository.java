package com.kenstudy.account_service.repository;

import com.kenstudy.account_service.model.Accounts;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public interface AccountRepository extends ReactiveCrudRepository<Accounts, Integer> {
 Mono<Boolean> existsByCustomersId(Integer customerId);

}
