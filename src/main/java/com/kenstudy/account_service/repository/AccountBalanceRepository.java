package com.kenstudy.account_service.repository;

import com.kenstudy.account_service.model.AccountBalance;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Mono;


public interface AccountBalanceRepository extends ReactiveCrudRepository<AccountBalance, Integer> {
    @Query("SELECT * FROM acct_balance WHERE account_id = ? ORDER BY recorded_at DESC LIMIT 1")
    Mono<AccountBalance> findLatestBalanceByAccountId(Integer accountId);


}
