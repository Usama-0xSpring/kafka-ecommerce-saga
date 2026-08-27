package com.kafkalearn.order.repository;

import com.kafkalearn.order.entity.Order;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, String> {
}
