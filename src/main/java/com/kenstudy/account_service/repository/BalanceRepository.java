package com.kenstudy.account_service.repository;

import com.kenstudy.account_service.model.Balance;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public interface BalanceRepository extends ReactiveCrudRepository<Balance, Integer> {

    @Query("SELECT * FROM acct_balance WHERE account_id = :accountId ORDER BY recorded_at DESC LIMIT 1")
    Mono<Balance> findLatestBalanceByAccountId(Integer accountId);

//    @Query("SELECT * FROM acct_balance WHERE account_id = :accountId")
//    Mono<Balance> findByIdCustom(Integer accountId);

}
