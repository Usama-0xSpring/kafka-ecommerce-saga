package com.kafkalearn.payment.kafka;

import com.kafkalearn.common.events.KafkaTopics;
import com.kafkalearn.common.events.OrderCreatedEvent;
import com.kafkalearn.common.events.PaymentFailedEvent;
import com.kafkalearn.common.events.PaymentSuccessEvent;
import com.kafkalearn.common.events.RefundRequestedEvent;
import com.kafkalearn.payment.entity.Payment;
import com.kafkalearn.payment.entity.PaymentStatus;
import com.kafkalearn.payment.repository.PaymentRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Listener methods take the raw ConsumerRecord (not the bare payload) because a
 * plain Object parameter gets bound to the record itself by Spring Kafka's
 * argument resolution, not the deserialized value - see record.value() below.
 */
@Component
public class PaymentEventListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventListener.class);

    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PaymentEventListener(PaymentRepository paymentRepository, KafkaTemplate<String, Object> kafkaTemplate) {
        this.paymentRepository = paymentRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = KafkaTopics.ORDER_EVENTS, groupId = "payment-service-group")
    public void onOrderCreated(ConsumerRecord<String, Object> record) {
        if (!(record.value() instanceof OrderCreatedEvent orderCreated)) {
            return;
        }

        log.info("Processing payment for orderId={}", orderCreated.orderId());

        // Simulated payment gateway: ~80% success rate.
        boolean approved = ThreadLocalRandom.current().nextInt(100) < 80;

        if (approved) {
            paymentRepository.save(new Payment(orderCreated.orderId(), orderCreated.amount(), PaymentStatus.SUCCESS, null));
            kafkaTemplate.send(KafkaTopics.PAYMENT_EVENTS, orderCreated.orderId(),
                    new PaymentSuccessEvent(orderCreated.orderId(), orderCreated.productId(),
                            orderCreated.quantity(), orderCreated.amount(), Instant.now()));
        } else {
            String reason = "Card declined by issuing bank";
            paymentRepository.save(new Payment(orderCreated.orderId(), orderCreated.amount(), PaymentStatus.FAILED, reason));
            kafkaTemplate.send(KafkaTopics.PAYMENT_EVENTS, orderCreated.orderId(),
                    new PaymentFailedEvent(orderCreated.orderId(), orderCreated.amount(), reason, Instant.now()));
        }
    }

    @KafkaListener(topics = KafkaTopics.REFUND_EVENTS, groupId = "payment-service-group")
    public void onRefundRequested(ConsumerRecord<String, Object> record) {
        if (!(record.value() instanceof RefundRequestedEvent refund)) {
            return;
        }

        // Compensating transaction: inventory couldn't fulfil the order after
        // payment succeeded, so payment-service reverses the charge.
        log.info("Refunding orderId={} amount={} reason={}", refund.orderId(), refund.amount(), refund.reason());
        paymentRepository.save(new Payment(refund.orderId(), refund.amount(), PaymentStatus.REFUNDED, refund.reason()));
    }
}
