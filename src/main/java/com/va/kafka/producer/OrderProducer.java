package com.va.kafka.producer;

import com.va.kafka.dto.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class OrderProducer {

    @Value("${kafka.topic.order}")
    private String orderTopic;

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrder(OrderEvent event) {
        // dùng orderId làm key -> cùng order luôn vào cùng partition
        kafkaTemplate.send(orderTopic, event.getOrderId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send order [{}]: {}", event.getOrderId(), ex.getMessage());
                        return;
                    }
                    log.info("Order sent [{}] -> partition: {}, offset: {}",
                            event.getOrderId(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset()
                    );
                });
    }
}