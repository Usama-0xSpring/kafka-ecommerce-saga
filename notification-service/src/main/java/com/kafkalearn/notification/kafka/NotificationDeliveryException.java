package com.kafkalearn.notification.kafka;

public class NotificationDeliveryException extends RuntimeException {

    public NotificationDeliveryException(String message) {
        super(message);
    }
}
