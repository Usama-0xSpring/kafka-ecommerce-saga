package com.kafkalearn.common.events;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderCreatedEvent(
        @JsonProperty("orderId") String orderId,
        @JsonProperty("customerId") String customerId,
        @JsonProperty("productId") String productId,
        @JsonProperty("quantity") int quantity,
        @JsonProperty("amount") BigDecimal amount,
        @JsonProperty("occurredAt") Instant occurredAt
) {
}
