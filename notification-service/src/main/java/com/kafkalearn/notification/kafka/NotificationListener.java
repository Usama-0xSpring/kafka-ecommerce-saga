package com.kafkalearn.notification.kafka;

import com.kafkalearn.common.events.*;
import com.kafkalearn.notification.entity.NotificationType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationListener {

    private final NotificationSender sender;

    public NotificationListener(NotificationSender sender) {
        this.sender = sender;
    }

    @KafkaListener(topics = KafkaTopics.ORDER_EVENTS, groupId = "notification-service-group")
    public void onOrderEvent(ConsumerRecord<String, Object> record) {
        if (record.value() instanceof OrderCreatedEvent created) {
            sender.send(created.orderId(), NotificationType.ORDER_PLACED,
                    "Your order " + created.orderId() + " has been placed. We'll email you once it's confirmed.");
        }
    }

    @KafkaListener(topics = KafkaTopics.PAYMENT_EVENTS, groupId = "notification-service-group")
    public void onPaymentEvent(ConsumerRecord<String, Object> record) {
        if (record.value() instanceof PaymentFailedEvent failed) {
            sender.send(failed.orderId(), NotificationType.PAYMENT_FAILED,
                    "Payment failed for order " + failed.orderId() + ": " + failed.reason());
        }
        // PaymentSuccessEvent doesn't need a customer-facing notification on its
        // own; we notify once inventory confirms the order below.
    }

    @KafkaListener(topics = KafkaTopics.INVENTORY_EVENTS, groupId = "notification-service-group")
    public void onInventoryEvent(ConsumerRecord<String, Object> record) {
        Object event = record.value();

        if (event instanceof InventoryReservedEvent reserved) {
            sender.send(reserved.orderId(), NotificationType.ORDER_CONFIRMED,
                    "Good news! Order " + reserved.orderId() + " is confirmed and will ship soon.");
        } else if (event instanceof InventoryOutOfStockEvent outOfStock) {
            sender.send(outOfStock.orderId(), NotificationType.ORDER_CANCELLED,
                    "Sorry, order " + outOfStock.orderId() + " was cancelled: item out of stock.");
        }
    }

    @KafkaListener(topics = KafkaTopics.REFUND_EVENTS, groupId = "notification-service-group")
    public void onRefundEvent(ConsumerRecord<String, Object> record) {
        if (record.value() instanceof RefundRequestedEvent refund) {
            sender.send(refund.orderId(), NotificationType.REFUND_ISSUED,
                    "A refund of " + refund.amount() + " has been issued for order " + refund.orderId() + ".");
        }
    }
}
