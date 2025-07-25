package com.kenstudy.account_service.service.customImpl;

import com.kenstudy.account.AccountBalanceDTO;
import com.kenstudy.account_service.config.client.AccountClient;
import com.kenstudy.account_service.exception.AccountNotFoundException;
import com.kenstudy.account_service.exception.ResourceNotFoundException;
import com.kenstudy.account_service.service.AccountService;
import com.kenstudy.customer.CustomerAccount;
import com.kenstudy.customer.CustomerResponseDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.List;

@Component
public class AccountDetailsHelper {
    private final AccountService accountService;
    private final AccountClient accountClient;

    @Autowired
    public AccountDetailsHelper(AccountService accountService, AccountClient accountClient) {
        this.accountService = accountService;
        this.accountClient = accountClient;
    }

    public Mono<CustomerResponseDTO> getCustomerAndAcctDetails(Integer accountId) {
        return Mono.justOrEmpty(accountId)
                .switchIfEmpty(Mono.error(new AccountNotFoundException("Account ID must not be null")))
                .flatMap(accountService::getAccountDetails)
                .switchIfEmpty(Mono.error(new AccountNotFoundException("Account details not found")))
                .flatMap(acct ->
                        accountClient.getCustomerDetails(acct.getCustomersId())
                                .map(customer -> mapCustomerAcctResponse(acct, customer))
                );
    }

    private CustomerResponseDTO mapCustomerAcctResponse(AccountBalanceDTO balDto, CustomerResponseDTO cusDto) {
        CustomerAccount account = new CustomerAccount();
        account.setBalance(balDto.getBalance());
        account.setAccountNumber(balDto.getAccountNumber());
        account.setAccountType(balDto.getAccountType());

        cusDto.setCustomerAccounts(List.of(account));
        cusDto.setAccountId(balDto.getAccountId());
        return cusDto;
    }
}
