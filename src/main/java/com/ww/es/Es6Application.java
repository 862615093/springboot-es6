package com.ww.es;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.ww.es.mapper")
public class Es6Application {
    public static void main(String[] args) {
        SpringApplication.run(Es6Application.class, args);
    }
}