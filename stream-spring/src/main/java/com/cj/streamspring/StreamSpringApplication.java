package com.cj.streamspring;


import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.cj.streamspring.mapper")
public class StreamSpringApplication {

    public static void main(String[] args) {
        SpringApplication.run(StreamSpringApplication.class, args);
    }

}
