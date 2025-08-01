package com.kenstudy.account_service.repository;

import com.kenstudy.account_service.model.Accounts;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@Repository
public interface AccountRepository extends ReactiveCrudRepository<Accounts, Integer> {
 Mono<Boolean> existsByCustomersId(Integer customerId);

 @Query("SELECT a FROM account a WHERE a.id IN (:ids)")
 Flux<Accounts> findSenderAndReceiverAccts(@Param("ids") List<Integer> ids);
}
