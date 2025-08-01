package com.kenstudy.account_service.model;

import com.kenstudy.account_service.exception.AccountNotFoundException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDate;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Table("balance")
@Slf4j
public class Balance {
    @Id
    private Integer id;
    private Double balance;
    private Integer accountId;
    private LocalDate recordedAt;

    public boolean checkAcctBalance(Double balance){
        double acctBal = this.balance - balance;
        return (acctBal > 5.00);
    }

    public Double debitAcctBalance(Double balance){
        return this.balance - balance;

    }

    public Double creditAccount(Double balance){
        return this.balance + balance;
    }

}
