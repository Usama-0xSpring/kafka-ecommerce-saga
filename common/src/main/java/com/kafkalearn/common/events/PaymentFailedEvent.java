package com.kafkalearn.common.events;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentFailedEvent(
        @JsonProperty("orderId") String orderId,
        @JsonProperty("amount") BigDecimal amount,
        @JsonProperty("reason") String reason,
        @JsonProperty("occurredAt") Instant occurredAt
) {
}
