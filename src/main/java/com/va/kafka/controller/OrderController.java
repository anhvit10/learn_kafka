package com.va.kafka.controller;

import com.va.kafka.dto.OrderEvent;
import com.va.kafka.producer.OrderProducer;
import com.va.kafka.request.OrderRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderProducer orderProducer;

    @PostMapping
    public ResponseEntity<String> placeOrder(@Valid @RequestBody OrderRequest request) {
        OrderEvent event = OrderEvent.builder()
                .orderId(UUID.randomUUID().toString())
                .customerId(request.getCustomerId())
                .product(request.getProduct())
                .quantity(request.getQuantity())
                .totalPrice(request.getTotalPrice())
                .status(OrderEvent.OrderStatus.PENDING)
                .createdAt(LocalDateTime.now())
                .build();

        orderProducer.sendOrder(event);

        return ResponseEntity.accepted().body("Order placed: " + event.getOrderId());
    }
}