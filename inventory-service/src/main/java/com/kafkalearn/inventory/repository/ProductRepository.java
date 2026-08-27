package com.kafkalearn.inventory.repository;

import com.kafkalearn.inventory.entity.Product;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductRepository extends JpaRepository<Product, String> {
}
