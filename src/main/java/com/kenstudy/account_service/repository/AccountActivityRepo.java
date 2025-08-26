package com.kenstudy.account_service.repository;

import com.kenstudy.account_service.model.AccountActivity;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface AccountActivityRepo extends ReactiveCrudRepository<AccountActivity, Integer> {

}
