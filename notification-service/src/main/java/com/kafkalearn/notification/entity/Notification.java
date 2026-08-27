package com.kafkalearn.notification.entity;

import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "notifications")
public class Notification {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String orderId;

    @Enumerated(EnumType.STRING)
    private NotificationType type;

    private String message;
    private Instant createdAt;

    protected Notification() {
    }

    public Notification(String orderId, NotificationType type, String message) {
        this.orderId = orderId;
        this.type = type;
        this.message = message;
        this.createdAt = Instant.now();
    }

    public Long getId() {
        return id;
    }

    public String getOrderId() {
        return orderId;
    }

    public NotificationType getType() {
        return type;
    }

    public String getMessage() {
        return message;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
