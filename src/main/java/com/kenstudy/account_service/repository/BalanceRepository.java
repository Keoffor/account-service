package com.kenstudy.account_service.repository;

import com.kenstudy.account_service.model.Balance;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Mono;


public interface BalanceRepository extends ReactiveCrudRepository<Balance, Integer> {
    @Query("SELECT * FROM balance WHERE account_id = ? ORDER BY recorded_at DESC LIMIT 1")
    Mono<Balance> findLatestBalanceByAccountId(Integer accountId);



}
