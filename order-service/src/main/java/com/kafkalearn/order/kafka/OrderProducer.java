package com.kafkalearn.order.kafka;

import com.kafkalearn.common.events.KafkaTopics;
import com.kafkalearn.common.events.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishOrderCreated(OrderCreatedEvent event) {
        // Keying by orderId keeps every event for the same order on the same
        // partition, so downstream consumers see them in order.
        kafkaTemplate.send(KafkaTopics.ORDER_EVENTS, event.orderId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish OrderCreatedEvent for orderId={}", event.orderId(), ex);
                    } else {
                        log.info("Published OrderCreatedEvent for orderId={} to partition={} offset={}",
                                event.orderId(), result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}
