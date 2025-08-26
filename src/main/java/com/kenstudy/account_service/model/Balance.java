package com.kenstudy.account_service.model;

import com.kenstudy.account_service.exception.AccountNotFoundException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDate;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Table("acct_balance")
@Slf4j
public class Balance {

    @Id
    private Integer id;

    private Double balance;

    @Column("account_id") // ✅ match the DB column name exactly
    private Integer accountId;

    @Column("recorded_at") // optional if DB column is snake_case
    private LocalDate recordedAt;

    public boolean checkAcctBalance(Double balance) {
        double acctBal = this.balance - balance;
        return (acctBal > 5.00);
    }

    public Double debitAcctBalance(Double amount) {
        this.balance = this.balance - amount; // ✅ update the actual field
        return this.balance;
    }

    public Double creditAccount(Double amount) {
        this.balance = this.balance + amount; // ✅ update the actual field
        return this.balance;
    }


}
