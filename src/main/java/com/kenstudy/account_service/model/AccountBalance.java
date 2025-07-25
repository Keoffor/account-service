package com.kenstudy.account_service.model;

import com.kenstudy.account_service.exception.AccountNotFoundException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDate;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Table("acct_balance")
public class AccountBalance {
    @Id
    private Integer id;
    private Double balance;
    private Integer accountId;
    private LocalDate recordedAt;

    public Double debitAcctBalance(Double balance){
        double acctBal = this.balance - balance;
        if (acctBal < 5.00) {
            throw new AccountNotFoundException("Account balance must not be less than $5 ");
        }
        return acctBal;

    }

    public Double creditAccount(Double balance){
        return this.balance + balance;
    }

}
