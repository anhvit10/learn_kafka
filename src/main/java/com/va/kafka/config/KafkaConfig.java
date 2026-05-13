package com.va.kafka.config;

import com.fasterxml.jackson.core.JsonParseException;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class KafkaConfig {

  private final KafkaProperties kafkaProperties;
  private final KafkaTemplate<String, Object> kafkaTemplate; // ← inject từ auto-configure

  @Value("${kafka.topic.order}")
  private String orderTopic;

  // ======= Topic =======
  @Bean
  public NewTopic orderTopic() {
    return TopicBuilder.name(orderTopic)
        .partitions(3)
        .replicas(1)
        .build();
  }

  @Bean
  public NewTopic orderDlqTopic() {
    return TopicBuilder.name(orderTopic + ".DLT")  // order-events.DLT
        .partitions(3)
        .replicas(1)
        .build();
  }

  // ======= Consumer Factory =======
  @Bean
  public ConsumerFactory<String, Object> consumerFactory() {
    Map<String, Object> props = new HashMap<>(
        kafkaProperties.buildConsumerProperties(null)
    );
    return new DefaultKafkaConsumerFactory<>(props);
  }

  // ======= Retry + DLQ =======
  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
      ConsumerFactory<String, Object> consumerFactory) {
    ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();

    factory.setConsumerFactory(consumerFactory);

    // retry 3 lần, mỗi lần cách nhau 2s -> 5s -> 10s (exponential backoff)
    factory.setCommonErrorHandler(errorHandler());
    return factory;
  }

  @Bean
  public DefaultErrorHandler errorHandler() {
    // exponential backoff: lần 1 sau 2s, lần 2 sau 5s, lần 3 sau 10s
    ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
    backOff.setInitialInterval(2_000L);   // 2 giây
    backOff.setMultiplier(2.5);           // nhân 2.5 mỗi lần
    backOff.setMaxInterval(10_000L);      // tối đa 10 giây

    // sau khi hết retry -> đẩy vào DLQ
    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
        kafkaTemplate,
        (record, ex) -> {
          log.error("Message failed after retries. Topic: {}, Offset: {}, Error: {}",
              record.topic(), record.offset(), ex.getMessage());
          // tự động gửi vào topic: order-events.DLT
          return new TopicPartition(record.topic() + ".DLT", record.partition());
        }
    );

    DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

    // không retry với lỗi do data sai (retry cũng vô ích)
    errorHandler.addNotRetryableExceptions(
        JsonParseException.class,          // JSON sai format
        IllegalArgumentException.class     // data không hợp lệ
    );

    return errorHandler;
  }
}