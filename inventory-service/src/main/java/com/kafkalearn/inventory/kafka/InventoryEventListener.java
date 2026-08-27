package com.kafkalearn.inventory.kafka;

import com.kafkalearn.common.events.InventoryOutOfStockEvent;
import com.kafkalearn.common.events.InventoryReservedEvent;
import com.kafkalearn.common.events.KafkaTopics;
import com.kafkalearn.common.events.PaymentSuccessEvent;
import com.kafkalearn.common.events.RefundRequestedEvent;
import com.kafkalearn.inventory.entity.Product;
import com.kafkalearn.inventory.repository.ProductRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * Listener method takes the raw ConsumerRecord (not the bare payload) because a
 * plain Object parameter gets bound to the record itself by Spring Kafka's
 * argument resolution, not the deserialized value - see record.value() below.
 */
@Component
public class InventoryEventListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventListener.class);

    private final ProductRepository productRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public InventoryEventListener(ProductRepository productRepository, KafkaTemplate<String, Object> kafkaTemplate) {
        this.productRepository = productRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = KafkaTopics.PAYMENT_EVENTS, groupId = "inventory-service-group")
    @Transactional
    public void onPaymentSuccess(ConsumerRecord<String, Object> record) {
        if (!(record.value() instanceof PaymentSuccessEvent success)) {
            // Ignore PaymentFailedEvent on this topic; inventory only acts once payment clears.
            return;
        }

        Product product = productRepository.findById(success.productId()).orElse(null);

        if (product != null && product.getStock() >= success.quantity()) {
            product.setStock(product.getStock() - success.quantity());
            productRepository.save(product);

            log.info("Reserved {} unit(s) of {} for orderId={}", success.quantity(), success.productId(), success.orderId());
            kafkaTemplate.send(KafkaTopics.INVENTORY_EVENTS, success.orderId(),
                    new InventoryReservedEvent(success.orderId(), success.productId(), success.quantity(), Instant.now()));
        } else {
            log.warn("Out of stock: productId={} requested={} for orderId={}",
                    success.productId(), success.quantity(), success.orderId());

            kafkaTemplate.send(KafkaTopics.INVENTORY_EVENTS, success.orderId(),
                    new InventoryOutOfStockEvent(success.orderId(), success.productId(), success.quantity(),
                            success.amount(), Instant.now()));

            // Compensating transaction: payment already succeeded, so ask payment-service to refund it.
            kafkaTemplate.send(KafkaTopics.REFUND_EVENTS, success.orderId(),
                    new RefundRequestedEvent(success.orderId(), success.amount(), "Item out of stock", Instant.now()));
        }
    }
}
