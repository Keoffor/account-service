package com.kenstudy.account_service.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Table("acct_activity")
public class AccountActivity {
    @Id
    private Integer id;
    private Integer acctId;
    private Integer customerId;
    private Double amount;
    private String type;
    private String status;
    private LocalDateTime dateTime;
}
