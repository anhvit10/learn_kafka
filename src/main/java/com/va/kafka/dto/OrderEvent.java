package com.va.kafka.dto;

import lombok.*;

import java.time.LocalDateTime;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderEvent {

    private String orderId;
    private String customerId;
    private String product;
    private Integer quantity;
    private Double totalPrice;
    private OrderStatus status;
    private LocalDateTime createdAt;

    public enum OrderStatus {
        PENDING, CONFIRMED, CANCELLED
    }
}