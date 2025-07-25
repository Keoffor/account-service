package com.kenstudy.account_service.service;

import com.kenstudy.account.AccountBalanceDTO;
import com.kenstudy.account.DebitAndCreditResponseDTO;
import com.kenstudy.payment.PaymentRequestDTO;
import reactor.core.publisher.Mono;

public interface AccountService {

    Mono<AccountBalanceDTO> createAccount(String userId);

    Mono<AccountBalanceDTO> getAccountDetails(Integer accountId);

    Mono<DebitAndCreditResponseDTO> processTransferFundRequest(PaymentRequestDTO paymentRequestDTO);
}
