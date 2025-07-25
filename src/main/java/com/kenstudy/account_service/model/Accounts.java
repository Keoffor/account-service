package com.kenstudy.account_service.model;


import com.kenstudy.account_service.utils.AccountType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
@Data
@NoArgsConstructor
@AllArgsConstructor
@Table("account")
public class Accounts {
    @Id
    private Integer id;
    private String accountNumber;
    private String accountType;
    private Integer customersId;
    @Transient
    private List<AccountBalance> balance = new ArrayList<>();

    public List<AccountBalance> getBalance() {
        return balance == null ? new ArrayList<>(): balance;
    }

    public void initializeAccountBalance(double initialBalance) {
        AccountBalance balance = new AccountBalance();
        balance.setBalance(initialBalance);
        balance.setRecordedAt(LocalDate.now());

        this.balance.add(balance);
    }

}
