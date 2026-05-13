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
public class InventoryConsumer {

  @KafkaListener(
      topics = "${kafka.topic.order}",
      groupId = "${kafka.consumer.group.inventory}",  // inventory-group
      concurrency = "3"    // 3 thread, mỗi thread xử lý 1 partition
  )
  public void consume(
      @Payload OrderEvent event,
      @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
      @Header(KafkaHeaders.OFFSET) long offset
  ) {
    log.info("[Inventory] Received order [{}] - partition: {}, offset: {}",
        event.getOrderId(), partition, offset);

    processInventory(event);
  }

  private void processInventory(OrderEvent event) {
    log.info("[Inventory] Reducing stock - product: {}, quantity: {}",
        event.getProduct(), event.getQuantity());

    // TODO: gọi inventory service / update DB
  }
}