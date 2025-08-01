package com.kenstudy.account_service.saga;

import com.kenstudy.account_service.exception.ResourceNotFoundException;
import com.kenstudy.event.AccountEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

@Component
@Slf4j
public class AccountPublisher {

    @Autowired
    private Sinks.Many<AccountEvent> accountSinks;

    public Mono<Void> publishAccountEvent(AccountEvent event) {
        Sinks.EmitResult result = accountSinks.tryEmitNext(event);

        if (result.isFailure()) {
            log.error(" Failed to emit AccountEvent:::====:::::: {} ", result.name());
            return Mono.error(new ResourceNotFoundException("Failed to emit AccountEvent: " + result.name()));
        }
        return Mono.empty();
    }

}
