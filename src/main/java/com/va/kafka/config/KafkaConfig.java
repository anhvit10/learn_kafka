package com.va.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Value("${kafka.topic.order}")
    private String orderTopic;

    @Bean
    public NewTopic orderTopic() {
        return TopicBuilder.name(orderTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}