package com.kafkalearn.common.events;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;

public record InventoryOutOfStockEvent(
        @JsonProperty("orderId") String orderId,
        @JsonProperty("productId") String productId,
        @JsonProperty("quantity") int quantity,
        @JsonProperty("refundAmount") BigDecimal refundAmount,
        @JsonProperty("occurredAt") Instant occurredAt
) {
}
