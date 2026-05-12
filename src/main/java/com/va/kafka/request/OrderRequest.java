package com.va.kafka.request;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class OrderRequest {

    @NotBlank(message = "customerId must not be blank")
    private String customerId;

    @NotBlank(message = "product must not be blank")
    private String product;

    @NotNull(message = "quantity must not be null")
    @Min(value = 1, message = "quantity must be at least 1")
    private Integer quantity;

    @NotNull(message = "totalPrice must not be null")
    @DecimalMin(value = "0.0", inclusive = false, message = "totalPrice must be greater than 0")
    private Double totalPrice;
}