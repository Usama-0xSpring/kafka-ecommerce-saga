package com.kafkalearn.order.entity;

import jakarta.persistence.*;

import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "orders")
public class Order {

    @Id
    @Column(name = "order_id", updatable = false, nullable = false)
    private String orderId;

    private String customerId;
    private String productId;
    private int quantity;
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    private OrderStatus status;

    private Instant createdAt;
    private Instant updatedAt;

    protected Order() {
    }

    public Order(String orderId, String customerId, String productId, int quantity, BigDecimal amount) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.productId = productId;
        this.quantity = quantity;
        this.amount = amount;
        this.status = OrderStatus.CREATED;
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public String getProductId() {
        return productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public void setStatus(OrderStatus status) {
        this.status = status;
        this.updatedAt = Instant.now();
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
