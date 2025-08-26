package com.kenstudy.account_service.saga;

import com.kenstudy.account_service.exception.BalanceNotFound;
import com.kenstudy.account_service.exception.ResourceNotFoundException;
import com.kenstudy.account_service.model.AccountActivity;
import com.kenstudy.account_service.model.Accounts;
import com.kenstudy.account_service.model.Balance;
import com.kenstudy.account_service.repository.AccountActivityRepo;
import com.kenstudy.account_service.repository.AccountRepository;
import com.kenstudy.account_service.repository.BalanceRepository;
import com.kenstudy.event.AccountEvent;
import com.kenstudy.event.CancelableEvent;
import com.kenstudy.event.TransactEvent;
import com.kenstudy.event.status.AccountStatus;
import com.kenstudy.event.status.TransStatus;
import com.kenstudy.payment.PaymentRequestDTO;
import com.kenstudy.transaction.TransferRequestDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@Component
@Slf4j
public class AccountHandler {
    private final BalanceRepository balanceRepo;
    private final AccountActivityRepo activityRepo;

    private final TransactionalOperator txOperator;
    private final R2dbcEntityTemplate template;
    private final  StreamBridge streamBridge;

    @Autowired
    public AccountHandler(BalanceRepository balanceRepo, AccountActivityRepo activityRepo,
                  TransactionalOperator txOperator, R2dbcEntityTemplate template,
                          StreamBridge streamBridge) {
        this.balanceRepo = balanceRepo;
        this.activityRepo = activityRepo;

        this.txOperator = txOperator;
        this.template = template;
        this.streamBridge = streamBridge;
    }

    public Mono<AccountEvent> processTransfer(TransactEvent transactEvent) {
        AccountEvent accountEvent = new AccountEvent();
        accountEvent.setCustomerEventId(transactEvent.getCustomerEventId());
        PaymentRequestDTO dto = mapToAccountDto(transactEvent.getTransRequestDTO());

        log.warn("correlationId Update {} ", accountEvent.getCustomerEventId());
        accountEvent.setCustomerEventId(accountEvent.getCustomerEventId());

        // Pre-check for null/empty event
        if (ObjectUtils.isEmpty(transactEvent) || transactEvent.isEventClosed() ||
                transactEvent.isError() || ObjectUtils.isEmpty(transactEvent.getCustomerEventId())) {
            log.warn("Received closed or empty Transaction Event. Skipping request. Event: {}", transactEvent);
            if (ObjectUtils.isNotEmpty(transactEvent)) {
                return handleFailureEvent(accountEvent, dto, "Event already closed or empty. Skipping request.");
            }
            return Mono.empty();
        }
        // Validate required fields
        if (ObjectUtils.isEmpty(dto.getAccountId()) || ObjectUtils.isEmpty(dto.getRecipientId()) ||
                ObjectUtils.isEmpty(dto.getCustomerId()) || ObjectUtils.isEmpty(dto.getRecipientAcctId())) {
            return handleFailureEvent(accountEvent, dto, "Missing required transaction fields");
        }
        // Check transaction status
        if (TransStatus.TRANSACTION_FAILED.equals(transactEvent.getTransStatus())) {
            return handleFailureEvent(accountEvent, dto, "Transaction status indicates transfer failure");
        }

        //Happy path - process transfer request debit sender account and credit receiver account
        return findSenderAndReceiverAccts(Arrays.asList(dto.getAccountId(),
                dto.getRecipientAcctId())).collectMap(Accounts::getId)
                .switchIfEmpty(Mono.error(new ResourceNotFoundException("Sender or Receiver account not found")))
                .flatMap(accountMap -> {
            Accounts sender = accountMap.get(dto.getAccountId());
            Accounts receiver = accountMap.get(dto.getRecipientAcctId());


            if (sender == null || receiver == null) {
                return handleFailureEvent(
                        accountEvent, dto, "Sender or receiver account not found in database");
            }

            return verifyCustomer(sender, dto.getCustomerId()).flatMap(isValidSender -> {
                if (!isValidSender) {
                    return handleFailureEvent(accountEvent, dto, "Customer ID does not match sender account");
                }
                return verifyCustomer(receiver, dto.getRecipientId()).flatMap(isValidReceiver -> {
                    if (!isValidReceiver) {
                    return handleFailureEvent(accountEvent, dto, "Recipient ID does not match recipient account");
                    }
                    // Proceed with transfer request
                    return transferFund(dto, accountEvent);
                });
            });
        }).as(txOperator::transactional);
    }

    private Mono<AccountEvent> transferFund(PaymentRequestDTO dto, AccountEvent event) {
        Mono<Balance> debit = findByCustom(dto.getAccountId())
                .switchIfEmpty(Mono.error(new BalanceNotFound("Sender account not found")))
                .flatMap(balance -> {
                    // Check and verify sender has sufficient balance
                    if (!balance.checkAcctBalance(dto.getAmount())) {
                        return Mono.defer(() ->
                            handleFailureEvent(event, dto, "Insufficient account balance")
                            .flatMap(failureEvent -> Mono.fromRunnable(() ->
                                streamBridge.send("accountEventFailure-out-0", failureEvent))
                                .then(Mono.error(new BalanceNotFound("Insufficient account balance"))))
                        );
                    }
                    balance.debitAcctBalance(dto.getAmount());
                    balance.setRecordedAt(LocalDate.now());
                    return balanceRepo.save(balance);
                });

        Mono<Balance> credit = findByCustom(dto.getRecipientAcctId())
                .switchIfEmpty(Mono.error(new BalanceNotFound("Recipient account not found")))
                .flatMap(resAcct -> {
                    resAcct.creditAccount(dto.getAmount());
                    resAcct.setRecordedAt(LocalDate.now());
                    return balanceRepo.save(resAcct);
                });


        return Mono.zip(debit, credit)
                .flatMap(tuple -> {
                    AccountActivity debited = mapAcctActivity(tuple.getT1(), dto.getAmount(), dto.getCustomerId(), "Debited");
                    AccountActivity credited = mapAcctActivity(tuple.getT2(), dto.getAmount(), dto.getRecipientId(), "Credited");

                    return activityRepo.saveAll(Flux.just(debited, credited))
                            .then(Mono.just(dto))
                            .map(dto1 -> this.mapToAcctEvent(dto1, event));
                })
                .doOnNext(n -> log.info("successfully processed transfer request {} ", n))
                .onErrorResume(ex -> {
                    log.error("Transfer processing error for DTO {}: {}", dto, ex.getMessage(), ex);
                    String reason = (ex.getMessage() != null) ? ex.getMessage() : "Unexpected transfer processing error";
                    return handleFailureEvent(event, dto, reason)
                            .doOnNext(failure -> streamBridge.send("accountEventFailure-out-0", failure));
                });
    }



    private AccountEvent mapToAcctEvent(PaymentRequestDTO dto, AccountEvent event) {
        dto.setTransactStatus(AccountStatus.PAYMENT_COMPLETED.name());
        event.setEventClosed(true);
        event.setError(false);
        event.setErrorMessage(null);
        event.setStatus(AccountStatus.PAYMENT_COMPLETED);
        event.setRequestDTO(dto);
        return event;
    }

//    private Function<Mono<PaymentRequestDTO>, Mono<AccountEvent> > transformDto () {
//        return dto -> dto.flatMap( dtos -> {
//             AccountEvent event = new AccountEvent();
//                    event.setAccountStatus(AccountStatus.PAYMENT_COMPLETED);
//                    event.setPaymentRequestDTO(dtos);
//                    return Mono.just(event);
//                });
//    }

    private AccountActivity mapAcctActivity(Balance bal, Double amt, Integer customerId, String type) {
        AccountActivity act = new AccountActivity();
        act.setAcctId(bal.getAccountId());
        act.setCustomerId(customerId);
        act.setAmount(amt);
        act.setType(type);
        act.setClosed(true);
        act.setStatus(AccountStatus.PAYMENT_COMPLETED.name());
        act.setDateTime(LocalDateTime.now());
        return act;
    }

    private Mono<Boolean> verifyCustomer(Accounts user, Integer userId) {
        return Mono.just(user.getCustomersId().equals(userId));
    }

    public PaymentRequestDTO mapToAccountDto(TransferRequestDTO transDtos) {
        PaymentRequestDTO dto = new PaymentRequestDTO();
        dto.setRecipientId(transDtos.getRecipientId());
        dto.setCustomerId(transDtos.getSenderId());
        dto.setTransactionId(transDtos.getTransactId());
        dto.setAmount(transDtos.getAmount());
        dto.setAccountId(transDtos.getSenderAcctId());
        dto.setRecipientAcctId(transDtos.getRecipientAcctId());
        dto.setDescription(transDtos.getDescription());
        return dto;
    }


    public <E extends CancelableEvent<D, S>, D, S> E cancelEvent(E event, D dto, String reason, S failure) {
        log.warn("Cancelling event due to: {}", reason);
        event.setRequestDTO(dto);
        event.setErrorMessage(reason);
        event.setError(true);
        event.setStatus(failure);
        event.setEventClosed(true);
        return event;
    }

    public Mono<Void> recordFailedTransact(PaymentRequestDTO dto) {
        AccountActivity acct = new AccountActivity();
        acct.setDateTime(LocalDateTime.now());
        acct.setAcctId(dto.getAccountId());
        acct.setAmount(dto.getAmount());
        acct.setClosed(true);
        acct.setCustomerId(dto.getCustomerId());
        acct.setStatus(AccountStatus.PAYMENT_FAILED.name());
        acct.setType("Incomplete");

        return activityRepo.save(acct)
                .doOnSuccess(saved ->
                        log.info("Saved failed transaction request for account {}", saved.getId()))
                .doOnError(error ->
                        log.error("Failed to save account activity: {}", error.getMessage()))
                .then();
    }


    public Flux<Accounts> findSenderAndReceiverAccts(List<Integer> ids) {
        return template.select(Accounts.class).matching(Query.query(Criteria.where("id").in(ids))).all();
    }
    public Mono<Balance> findByCustom(Integer accountId) {
        return template.select(Balance.class)
                .matching(Query.query(Criteria.where("account_id").is(accountId)))
                .first();
    }



    public Mono<AccountEvent> handleFailureEvent(AccountEvent failureEvent, PaymentRequestDTO dto, String reason) {
        log.error(reason);
        dto.setTransactStatus(AccountStatus.PAYMENT_FAILED.name());
        return recordFailedTransact(dto)
                .thenReturn(cancelEvent(failureEvent, dto, reason, AccountStatus.PAYMENT_FAILED));

    }

}