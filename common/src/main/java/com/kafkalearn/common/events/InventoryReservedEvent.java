package com.kafkalearn.common.events;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public record InventoryReservedEvent(
        @JsonProperty("orderId") String orderId,
        @JsonProperty("productId") String productId,
        @JsonProperty("quantity") int quantity,
        @JsonProperty("occurredAt") Instant occurredAt
) {
}
