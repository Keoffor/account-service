package com.kenstudy.account_service.service.customImpl;

import com.kenstudy.account.AccountBalanceDTO;
import com.kenstudy.account.DebitAndCreditResponseDTO;
import com.kenstudy.account_service.exception.AccountNotFoundException;
import com.kenstudy.account_service.exception.BalanceNotFound;
import com.kenstudy.account_service.model.AccountBalance;
import com.kenstudy.account_service.model.Accounts;
import com.kenstudy.account_service.repository.AccountBalanceRepository;
import com.kenstudy.account_service.repository.AccountRepository;
import com.kenstudy.account_service.service.AccountService;
import com.kenstudy.account_service.utils.AccountType;
import com.kenstudy.account_service.utils.CustomerExistsHelper;
import com.kenstudy.payment.PaymentRequestDTO;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.util.Random;

@Service
public class AccountServiceImpl implements AccountService {

    private final AccountRepository accountRepository;
    private final AccountBalanceRepository balanceRepository;
    private final CustomerExistsHelper customerExistsHelper;
    private final TransactionalOperator txOperator;


    public AccountServiceImpl(AccountRepository accountRepository,
              AccountBalanceRepository balanceRepository, CustomerExistsHelper customerExistsHelper,
                              TransactionalOperator txOperator) {
        this.accountRepository = accountRepository;
        this.balanceRepository = balanceRepository;
        this.customerExistsHelper = customerExistsHelper;
        this.txOperator = txOperator;
    }

    @Override
    public Mono<AccountBalanceDTO> createAccount(String userId) {
        return Mono.justOrEmpty(userId)
                .map(this::mapToAccountEntity)
                .flatMap(accountRepository::save)
                .flatMap(this::mapToCreateBalanceDTO)
                .as(txOperator::transactional);

    }

    @Override
    public Mono<AccountBalanceDTO> getAccountDetails(Integer accountId) {
        return Mono.justOrEmpty(accountId)
                .switchIfEmpty(Mono.error(new AccountNotFoundException("Invalid Account Id")))
                .flatMap(accountRepository::findById)
                .switchIfEmpty(Mono.error(new AccountNotFoundException("Account with Id Number " + accountId + " does not exist")))
                .flatMap(account ->
                        balanceRepository.findLatestBalanceByAccountId(account.getId())
                                .switchIfEmpty(Mono.error(new BalanceNotFound("Balance not found")))
                                .map(balance -> mapToGetBalance(account, balance))
                );
    }

    @Override
    public Mono<DebitAndCreditResponseDTO> processTransferFundRequest(PaymentRequestDTO paymentRequestDTO) {
        if (ObjectUtils.isEmpty(paymentRequestDTO.getRecipientId())) {
            return Mono.error(new AccountNotFoundException("recipient id must not be empty"));
        }
        return Mono.justOrEmpty(paymentRequestDTO.getAccountId())
                .switchIfEmpty(Mono.error(new AccountNotFoundException("account id must not be empty")))
                .flatMap(this::getAccountDetails)
                .flatMap(acct -> transferFund(acct, paymentRequestDTO))
                .as(txOperator::transactional);
    }


    private Mono<DebitAndCreditResponseDTO> transferFund(AccountBalanceDTO acctBalDTO,
                                                         PaymentRequestDTO payResquestDto) {
        return customerExistsHelper.checkCustomerExists(payResquestDto.getCustomerId(), acctBalDTO.getAccountId())
                .flatMap(exists -> {
                    if (!exists) {
                        return Mono.error(
                                new AccountNotFoundException("Account Number " + acctBalDTO.getAccountNumber() + " " +
                                        "with Customer id " + payResquestDto.getCustomerId() + "" +
                                        " does not exist"));
                    }

                    if (acctBalDTO.getBalance() < 5.00) {
                        return Mono.error(new BalanceNotFound("Error: Insufficient balance"));
                    }

                    Mono<AccountBalance> debit = balanceRepository.findById(acctBalDTO.getAccountId())
                            .switchIfEmpty(Mono.error(new AccountNotFoundException("Account Id does not exist")))
                            .flatMap(balance -> {
                                double updatedDebitBalance = balance.debitAcctBalance(payResquestDto.getAmount());
                                balance.setBalance(updatedDebitBalance);
                                balance.setRecordedAt(LocalDate.now());
                                return balanceRepository.save(balance);
                            });

                    Mono<AccountBalance> credit = balanceRepository.findById(payResquestDto.getRecipientId())
                            .switchIfEmpty(Mono.error(new BalanceNotFound("Recipient with Account Id "
                                    + payResquestDto.getRecipientId() + " does not exist")))
                            .flatMap(recipientAcct -> {
                                double updatedCredit = recipientAcct.creditAccount(payResquestDto.getAmount());
                                recipientAcct.setBalance(updatedCredit);
                                recipientAcct.setRecordedAt(LocalDate.now());
                                return balanceRepository.save(recipientAcct);
                            });
                    return Mono.zip(debit, credit, Mono.just(payResquestDto))
                            .map(tuple -> mapPaymentResDto(tuple.getT1(),
                                    tuple.getT2(), tuple.getT3()));
                });
    }


    private String generateAccountNumber() {
        // Example: generates a 12-digit random number starting with a fixed prefix
        String prefix = "30";
        String randomNumber = String.format("%010d", new Random().nextLong() % 1_000_000_0000L);
        return prefix + randomNumber.replace("-", ""); // remove negative if any
    }

    // Helper to map DTO to entity
    private Accounts mapToAccountEntity(String userId) {
        Accounts accounts = new Accounts();
        accounts.setCustomersId(Integer.parseInt(userId));
        accounts.setAccountNumber(generateAccountNumber());
        accounts.setAccountType(String.valueOf(AccountType.CHECKING));
        return accounts;
    }

    // Helper to create and save account balance
    @Transactional
    private Mono<AccountBalanceDTO> mapToCreateBalanceDTO(Accounts savedAccount) {
        AccountBalance balance = new AccountBalance();
        savedAccount.initializeAccountBalance(10.00);
        AccountBalance initialBalance = savedAccount.getBalance().getFirst();
        balance.setAccountId(savedAccount.getId());
        balance.setBalance(initialBalance.getBalance());
        balance.setRecordedAt(initialBalance.getRecordedAt());
        return balanceRepository.save(balance)
                .map(newAccount -> {
                    AccountBalanceDTO dto = new AccountBalanceDTO();
                    dto.setAccountId(savedAccount.getId());
                    dto.setAccountNumber(savedAccount.getAccountNumber());
                    dto.setBalance(newAccount.getBalance());
                    dto.setAccountType(savedAccount.getAccountType());
                    dto.setCustomersId(savedAccount.getCustomersId());
                    dto.setRecordedAt(newAccount.getRecordedAt());
                    return dto;
                });
    }

    private DebitAndCreditResponseDTO mapPaymentResDto(AccountBalance debit, AccountBalance credit,
                                                       PaymentRequestDTO paymentRequestDTO) {
        DebitAndCreditResponseDTO dto = new DebitAndCreditResponseDTO();
        dto.setSenderAccountId(debit.getAccountId());
        dto.setAmount(paymentRequestDTO.getAmount());
        dto.setRecipientId(credit.getAccountId());
        dto.setCreatedDate(LocalDate.now());
        return dto;
    }


    private AccountBalanceDTO mapToGetBalance(Accounts accounts, AccountBalance balance) {
        AccountBalanceDTO balanceDTO = new AccountBalanceDTO();
        balanceDTO.setAccountId(accounts.getId());
        balanceDTO.setAccountNumber(accounts.getAccountNumber());
        balanceDTO.setAccountType(accounts.getAccountType());
        balanceDTO.setBalance(balance.getBalance());
        balanceDTO.setCustomersId(accounts.getCustomersId());
        balanceDTO.setRecordedAt(balance.getRecordedAt());
        return balanceDTO;

    }


}
