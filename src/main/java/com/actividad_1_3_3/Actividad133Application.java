package com.actividad_1_3_3;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class Actividad133Application {

    public static void main(String[] args) {
        SpringApplication.run(Actividad133Application.class, args);
    }
}
