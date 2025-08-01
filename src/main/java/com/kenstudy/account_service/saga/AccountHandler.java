package com.kenstudy.account_service.saga;

import com.kenstudy.account_service.exception.AccountNotFoundException;
import com.kenstudy.account_service.exception.BalanceNotFound;
import com.kenstudy.account_service.exception.ResourceNotFoundException;
import com.kenstudy.account_service.model.AccountActivity;
import com.kenstudy.account_service.model.Balance;
import com.kenstudy.account_service.model.Accounts;
import com.kenstudy.account_service.repository.AccountActivityRepo;
import com.kenstudy.account_service.repository.BalanceRepository;
import com.kenstudy.account_service.repository.AccountRepository;
import com.kenstudy.event.AccountEvent;
import com.kenstudy.event.TransactEvent;
import com.kenstudy.event.status.AccountStatus;
import com.kenstudy.event.status.TransStatus;
import com.kenstudy.payment.PaymentRequestDTO;
import com.kenstudy.transaction.TransferRequestDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

@Component
@Slf4j
public class AccountHandler {
    private final AccountRepository accountRepo;
    private final BalanceRepository balanceRepo;
    private final AccountActivityRepo activityRepo;
    private final AccountPublisher accountPublisher;

    @Autowired
    public AccountHandler(AccountRepository accountRepo, BalanceRepository balanceRepo,
              AccountActivityRepo activityRepo, AccountPublisher accountPublisher) {
        this.accountRepo = accountRepo;
        this.balanceRepo = balanceRepo;
        this.activityRepo = activityRepo;
        this.accountPublisher = accountPublisher;
    }

    public Mono<AccountEvent> processTransfer(TransactEvent transactEvent) {
        AccountEvent accountEvent = new AccountEvent();
        PaymentRequestDTO dto = mapToAccountDto(transactEvent.getTransRequestDTO());

        if (ObjectUtils.isEmpty(transactEvent)) {
            log.error("TransactEvent is null or empty. Cannot proceed with transaction.");
            return cancelTransfer(dto,"TransactEvent is null or empty. Cannot proceed with transaction.");
        }
        if (ObjectUtils.isEmpty(dto.getAccountId()) || ObjectUtils.isEmpty(dto.getRecipientId()) ||
            ObjectUtils.isEmpty(dto.getCustomerId()) || ObjectUtils.isEmpty(dto.getRecipientAcctId())) {

            log.error("Missing required transaction fields in DTO:::===::: {}", dto);
            return cancelTransfer(dto,"Missing required transaction fields");
        }
        if (TransStatus.TRANSACTION_INITIATED.equals(transactEvent.getTransStatus())) {
            return accountRepo.findSenderAndReceiverAccts(Arrays.asList(dto.getAccountId(), dto.getRecipientAcctId()))
                .collectMap(Accounts::getId)
                .switchIfEmpty(Mono.error(new ResourceNotFoundException("Sender or Receiver account not found")))
                .flatMap(accountMap -> {
                Accounts sender = accountMap.get(dto.getAccountId());
                Accounts receiver = accountMap.get(dto.getRecipientAcctId());

                if (sender == null || receiver == null) {
                    log.error("Sender or receiver account not found in database");
                    return cancelTransfer(dto, "Sender or receiver account not found in database");
                }
                return verifyCustomer(sender, dto.getCustomerId())
                    .flatMap(isValidSender -> {
                        if (!isValidSender) {
                            log.error("Customer ID does not match sender account");
                            return cancelTransfer(dto,"Customer ID does not match sender account");
                        }

                        return verifyCustomer(receiver, dto.getRecipientId())
                            .flatMap(isValidReceiver -> {
                                if (!isValidReceiver) {
                                    log.error("Recipient ID does not match recipient account");
                                    return cancelTransfer(dto,"Recipient ID does not match recipient account");
                                }
                            // Proceed with transfer
                                    return transferFund(dto, accountEvent);
                                });
                        });
            });
        }
        return cancelTransfer(dto, "Transaction status is not INITIATED");
    }

    private Mono<AccountEvent> transferFund(PaymentRequestDTO dto, AccountEvent event) {
        Mono<Balance> debit = balanceRepo.findById(dto.getAccountId())
                .switchIfEmpty(Mono.error(new BalanceNotFound("Sender account not found")))
                .flatMap(balance -> {
                    if (!balance.checkAcctBalance(dto.getAmount())) {
                        event.setErrorMessage("Insufficient account balance");
                        event.setPaymentRequestDTO(dto);
                        event.setAccountStatus(AccountStatus.PAYMENT_FAILED);
                        accountPublisher.publishAccountEvent(event);
                        return Mono.error(new BalanceNotFound("Insufficient balance"));
                    }
                    double updatedDebitBalance = balance.debitAcctBalance(dto.getAmount());
                    balance.setBalance(updatedDebitBalance);
                    balance.setRecordedAt(LocalDate.now());
                    return balanceRepo.save(balance);
                });

        Mono<Balance> credit = balanceRepo.findById(dto.getRecipientAcctId())
                .switchIfEmpty(Mono.error(new BalanceNotFound("Recipient account not found")))
                .flatMap(resAcct -> {
                    resAcct.setBalance(resAcct.creditAccount(dto.getAmount()));
                    resAcct.setRecordedAt(LocalDate.now());
                    return balanceRepo.save(resAcct);
                });
        Mono<AccountActivity> debited = debit.map(b -> mapAcctActivity(b, dto.getAmount(), "Debited"));
        Mono<AccountActivity> credited = credit.map(b -> mapAcctActivity(b, dto.getAmount(), "Credited"));

        return Mono.zip(debit, credit)
                .flatMap(t -> activityRepo.saveAll(Flux.concat(debited, credited))
                        .thenReturn(dto))
                .map(this::maptoAcctEvent);
    }

    private AccountEvent maptoAcctEvent (PaymentRequestDTO dto) {
        AccountEvent event = new AccountEvent();
        event.setAccountStatus(AccountStatus.PAYMENT_COMPLETED);
        event.setPaymentRequestDTO(dto);
        return event;
    }

private AccountActivity mapAcctActivity(Balance bal, Double amt, String type){
    AccountActivity act = new AccountActivity();
    act.setAcctId(bal.getAccountId());
    act.setAmount(amt);
    act.setType(type);
    act.setStatus(AccountStatus.PAYMENT_COMPLETED.name());
    act.setDateTime(LocalDateTime.now());
    return act;
}
private Mono<Boolean> verifyCustomer(Accounts user, Integer userId) {
    return Mono.just(user.getCustomersId().equals(userId));
}

private PaymentRequestDTO mapToAccountDto(TransferRequestDTO transDtos) {
    PaymentRequestDTO dto = new PaymentRequestDTO();
    dto.setRecipientId(transDtos.getRecipientId());
    dto.setCustomerId(transDtos.getSenderId());
    dto.setTransactionId(transDtos.getTransactId());
    dto.setAmount(transDtos.getAmount());
    dto.setAccountId(transDtos.getSenderId());
    dto.setRecipientAcctId(transDtos.getRecipientAcctId());
    return dto;
}

    private Mono<AccountEvent> cancelTransfer(PaymentRequestDTO dto, String reason) {
        log.warn("Cancelling transaction due to: {}", reason);
        AccountEvent cancelEvent = new AccountEvent();
        cancelEvent.setPaymentRequestDTO(dto);
        cancelEvent.setErrorMessage(reason);
        cancelEvent.setAccountStatus(AccountStatus.PAYMENT_CANCELLED);
        return Mono.just(cancelEvent);
    }


public void updateTransactAcct(AccountEvent accountEvent) {
}
}
