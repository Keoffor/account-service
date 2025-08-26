package com.kenstudy.account_service.model;


import com.fasterxml.jackson.annotation.JsonIgnore;
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
    @JsonIgnore
    private List<Balance> balance = new ArrayList<>();

    public List<Balance> getBalance() {
        return balance == null ? new ArrayList<>(): balance;
    }

    public void initializeAccountBalance(double initialBalance) {
        Balance balance = new Balance();
        balance.setBalance(initialBalance);
        balance.setRecordedAt(LocalDate.now());

        this.balance.add(balance);
    }

}
