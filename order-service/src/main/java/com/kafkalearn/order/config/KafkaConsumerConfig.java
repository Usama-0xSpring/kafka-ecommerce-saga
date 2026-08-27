package com.kafkalearn.order.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Retries a failing listener 3 times (1s apart), then routes the record to
 * "<topic>.DLT" instead of blocking the partition forever. Spring Boot's
 * auto-configured listener container factory picks up this bean automatically.
 */
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (ConsumerRecord<?, ?> record, Exception ex) ->
                        new org.apache.kafka.common.TopicPartition(record.topic() + ".DLT", record.partition()));

        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L));
    }
}
