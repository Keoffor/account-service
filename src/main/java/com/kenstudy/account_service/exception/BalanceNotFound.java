package com.kenstudy.account_service.exception;

public class BalanceNotFound extends RuntimeException{

    public BalanceNotFound(String message){
        super(message);
    }
}
