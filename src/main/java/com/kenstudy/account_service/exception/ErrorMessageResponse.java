package com.kenstudy.account_service.exception;

import lombok.*;
import org.springframework.http.HttpStatus;
@Builder
@AllArgsConstructor
@Data

public class ErrorMessageResponse {
    private int status;
    private String error;
    private String message;
    private String path;

}
