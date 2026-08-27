package com.kafkalearn.order.kafka;

import com.kafkalearn.common.events.InventoryOutOfStockEvent;
import com.kafkalearn.common.events.InventoryReservedEvent;
import com.kafkalearn.common.events.PaymentFailedEvent;
import com.kafkalearn.common.events.PaymentSuccessEvent;
import com.kafkalearn.common.events.KafkaTopics;
import com.kafkalearn.order.entity.Order;
import com.kafkalearn.order.entity.OrderStatus;
import com.kafkalearn.order.repository.OrderRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * order-service runs its own consumer group, separate from payment-service and
 * inventory-service, so it independently tracks progress through payment-events
 * and inventory-events to move the order through its saga states.
 *
 * Listener methods take the raw ConsumerRecord (not the bare payload) because a
 * plain Object parameter gets bound to the record itself by Spring Kafka's
 * argument resolution, not the deserialized value - see record.value() below.
 */
@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    private final OrderRepository orderRepository;

    public OrderEventListener(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @KafkaListener(topics = KafkaTopics.PAYMENT_EVENTS, groupId = "order-service-group")
    public void onPaymentEvent(ConsumerRecord<String, Object> record) {
        Object event = record.value();

        if (event instanceof PaymentSuccessEvent success) {
            log.info("Payment succeeded for orderId={}", success.orderId());
            // No status change here: order stays CREATED until inventory confirms it.
        } else if (event instanceof PaymentFailedEvent failed) {
            log.info("Payment failed for orderId={}, reason={}", failed.orderId(), failed.reason());
            updateStatus(failed.orderId(), OrderStatus.PAYMENT_FAILED);
        }
    }

    @KafkaListener(topics = KafkaTopics.INVENTORY_EVENTS, groupId = "order-service-group")
    public void onInventoryEvent(ConsumerRecord<String, Object> record) {
        Object event = record.value();

        if (event instanceof InventoryReservedEvent reserved) {
            log.info("Inventory reserved for orderId={}", reserved.orderId());
            updateStatus(reserved.orderId(), OrderStatus.CONFIRMED);
        } else if (event instanceof InventoryOutOfStockEvent outOfStock) {
            log.info("Inventory out of stock for orderId={}", outOfStock.orderId());
            updateStatus(outOfStock.orderId(), OrderStatus.CANCELLED);
        }
    }

    private void updateStatus(String orderId, OrderStatus status) {
        orderRepository.findById(orderId).ifPresent(order -> {
            order.setStatus(status);
            orderRepository.save(order);
        });
    }
}
