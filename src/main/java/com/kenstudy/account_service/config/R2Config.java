package com.kenstudy.account_service.config;

import io.asyncer.r2dbc.mysql.MySqlConnectionConfiguration;
import io.asyncer.r2dbc.mysql.MySqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;


@Configuration
@EnableR2dbcRepositories
public class R2Config {

    @Value("${spring.r2dbc.url}")
    private String host;

    @Value("${spring.r2dbc.password}")
    private String password;

    @Value("${spring.r2dbc.username}")
    private String username;


    @Bean
    public R2dbcEntityTemplate r2dbcEntityTemplate(ConnectionFactory connectionFactory) {
        return new R2dbcEntityTemplate(connectionFactory);
    }

//    @Override
//    @Bean
//    public ConnectionFactory connectionFactory() {
//        MySqlConnectionConfiguration config = MySqlConnectionConfiguration.builder()
//                .host(host)
//                .username(username)
//                .password(password)
//                .port(3307)
//                .database("patient_db")
//                .build();
//
//        return MySqlConnectionFactory.from(config);
//    }


//    @Bean
//    public ReactiveTransactionManager reactiveTransactionManager() {
//        return new R2dbcTransactionManager(connectionFactory());
//    }
//
//    @Bean
//    public TransactionalOperator transactionalOperator() {
//        return TransactionalOperator.create(reactiveTransactionManager());
//    }


}
