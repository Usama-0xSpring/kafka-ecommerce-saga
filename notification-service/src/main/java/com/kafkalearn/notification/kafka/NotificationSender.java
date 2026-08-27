package com.kafkalearn.notification.kafka;

import com.kafkalearn.notification.entity.Notification;
import com.kafkalearn.notification.entity.NotificationType;
import com.kafkalearn.notification.repository.NotificationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

@Component
public class NotificationSender {

    private static final Logger log = LoggerFactory.getLogger(NotificationSender.class);

    private final NotificationRepository notificationRepository;

    public NotificationSender(NotificationRepository notificationRepository) {
        this.notificationRepository = notificationRepository;
    }

    public void send(String orderId, NotificationType type, String message) {
        // Simulated flaky delivery channel (~15% failure) so you can watch
        // DefaultErrorHandler retry the listener and, after 3 attempts,
        // publish the record to "<topic>.DLT" instead of blocking the partition.
        if (ThreadLocalRandom.current().nextInt(100) < 15) {
            throw new NotificationDeliveryException("Simulated delivery failure for orderId=" + orderId);
        }

        notificationRepository.save(new Notification(orderId, type, message));
        log.info("Notification sent: [{}] {}", type, message);
    }
}
