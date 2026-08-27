package com.kafkalearn.order.service;

import com.kafkalearn.common.events.OrderCreatedEvent;
import com.kafkalearn.order.dto.CreateOrderRequest;
import com.kafkalearn.order.entity.Order;
import com.kafkalearn.order.exception.OrderNotFoundException;
import com.kafkalearn.order.kafka.OrderProducer;
import com.kafkalearn.order.repository.OrderRepository;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderProducer orderProducer;

    public OrderService(OrderRepository orderRepository, OrderProducer orderProducer) {
        this.orderRepository = orderRepository;
        this.orderProducer = orderProducer;
    }

    public Order createOrder(CreateOrderRequest request) {
        String orderId = UUID.randomUUID().toString();

        Order order = new Order(orderId, request.customerId(), request.productId(),
                request.quantity(), request.amount());
        orderRepository.save(order);

        orderProducer.publishOrderCreated(new OrderCreatedEvent(
                orderId, request.customerId(), request.productId(),
                request.quantity(), request.amount(), Instant.now()));

        return order;
    }

    public Order getOrder(String orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
    }

    public List<Order> getAllOrders() {
        return orderRepository.findAll();
    }
}
