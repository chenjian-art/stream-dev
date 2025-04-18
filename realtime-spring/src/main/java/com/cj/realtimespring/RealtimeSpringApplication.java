package com.cj.realtimespring;


import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
@MapperScan(basePackages = "com.cj.realtimespring.mapper")
@SpringBootApplication
public class RealtimeSpringApplication {

    public static void main(String[] args) {
        SpringApplication.run(RealtimeSpringApplication.class, args);
    }

}
