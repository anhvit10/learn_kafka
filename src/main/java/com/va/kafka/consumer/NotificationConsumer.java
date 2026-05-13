package com.va.kafka.consumer;

import com.va.kafka.dto.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class NotificationConsumer {

  @KafkaListener(
      topics = "${kafka.topic.order}",
      groupId = "${kafka.consumer.group.notification}"  // notification-group
  )
  public void consume(
      @Payload OrderEvent event,
      @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
      @Header(KafkaHeaders.OFFSET) long offset
  ) {
    log.info("[Notification] Received order [{}] - partition: {}, offset: {}",
        event.getOrderId(), partition, offset);

    processNotification(event);
  }

  private void processNotification(OrderEvent event) {
    log.info("[Notification] Sending email to customer: {}", event.getCustomerId());

    // TODO: gọi email service / SMS service
  }
}