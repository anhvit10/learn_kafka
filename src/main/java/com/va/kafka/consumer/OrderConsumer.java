package com.va.kafka.consumer;

import com.va.kafka.dto.OrderEvent;
import com.va.kafka.exception.OrderProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class OrderConsumer {

  @KafkaListener(
      topics = "${kafka.topic.order}",
      groupId = "order-group",
      containerFactory = "kafkaListenerContainerFactory"
  )
  public void consume(
      @Payload OrderEvent event,
      @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
      @Header(KafkaHeaders.OFFSET) long offset
  ) {
    log.info("Received order [{}] from partition: {}, offset: {}",
        event.getOrderId(), partition, offset);

    this.processOrder(event);
  }

  private void processOrder(OrderEvent event) {
    // ❌ throw RuntimeException -> trigger retry
    if (event.getTotalPrice() > 10_000) {
      throw new OrderProcessingException(
          "Order [" + event.getOrderId() + "] exceeds price limit"
      );
    }

    // ❌ throw IllegalArgumentException -> KHÔNG retry (notRetryable)
    if (event.getQuantity() <= 0) {
      throw new IllegalArgumentException(
          "Invalid quantity for order: " + event.getOrderId()
      );
    }

    switch (event.getStatus()) {
      case PENDING -> log.info("Processing order [{}] for customer [{}] - {} x{}",
          event.getOrderId(), event.getCustomerId(),
          event.getProduct(), event.getQuantity());
      case CONFIRMED -> log.info("Order [{}] confirmed - Total: ${}",
          event.getOrderId(), event.getTotalPrice());
      case CANCELLED -> log.warn("Order [{}] was cancelled", event.getOrderId());
    }
  }
}