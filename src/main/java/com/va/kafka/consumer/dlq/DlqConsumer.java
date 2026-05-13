package com.va.kafka.consumer.dlq;

import com.va.kafka.dto.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class DlqConsumer {

  @KafkaListener(
      topics = "${kafka.topic.order}.DLT",   // order-events.DLT
      groupId = "order-dlq-group"
  )
  public void consume(
      @Payload OrderEvent event,
      @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
      @Header(KafkaHeaders.OFFSET) long offset,
      @Header("kafka_dlt-exception-message") String errorMessage   // lỗi gốc
  ) {
    log.error("=== DLQ MESSAGE ===");
    log.error("Topic: {}, Offset: {}", topic, offset);
    log.error("Error: {}", errorMessage);
    log.error("Order: {}", event.getOrderId());

    // TODO: alert Slack/email, lưu DB để xử lý thủ công...
  }
}