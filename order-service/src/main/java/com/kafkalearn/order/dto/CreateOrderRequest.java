package com.kafkalearn.order.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

public record CreateOrderRequest(
        @NotBlank(message = "customerId is required")
        String customerId,

        @NotBlank(message = "productId is required")
        String productId,

        @Positive(message = "quantity must be greater than 0")
        int quantity,

        @NotNull(message = "amount is required")
        @DecimalMin(value = "0.01", message = "amount must be greater than 0")
        BigDecimal amount
) {
}
