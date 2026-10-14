package com.finnode.payment.kafka;

import com.finnode.payment.event.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Publicador de eventos para el Saga de pagos.
 *
 * Encapsula la lógica de publicación a Kafka. Cada transición importante
 * del Saga publica un evento para que los demás microservicios reaccionen.
 * La key de todos los mensajes es el transactionId.
 *
 * Topics publicados:
 *  - fraud.check-requested → fraud-engine para evaluar el riesgo (primer paso de la saga)
 *  - payment.initiated     → account-service para reservar fondos (solo si el fraude fue aprobado)
 *  - payment.completed     → ledger-service para registrar asientos
 *  - payment.reversed      → account-service y ledger-service para compensar
 *  - payment.failed        → interesados: rechazo por fraude, fallo o compensación terminada
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Solicita a fraud-engine la evaluación de la transacción.
     *
     * Topic: fraud.check-requested
     * Consumidor: fraud-engine (Python / FastAPI)
     * Respuesta: fraud.check-completed (ver PaymentEventConsumer)
     */
    public void publishFraudCheckRequested(UUID transactionId, UUID userId,
                                           UUID sourceAccountId, UUID destinationAccountId,
                                           BigDecimal amount, String currency) {
        FraudCheckRequestedEvent event = new FraudCheckRequestedEvent(
                transactionId,
                userId,
                sourceAccountId,
                destinationAccountId,
                amount,
                currency,
                Instant.now()
        );

        kafkaTemplate.send("fraud.check-requested", transactionId.toString(), event);
        log.info("Published FraudCheckRequestedEvent for transaction: {}", transactionId);
    }

    /**
     * Publica el evento que inicia la reserva de fondos.
     *
     * Topic: payment.initiated
     * Consumidor: account-service
     *
     * Se publica únicamente cuando fraud-engine aprobó la transacción: señala
     * a account-service que reserve los fondos en la cuenta de origen.
     */
    public void publishPaymentInitiated(UUID transactionId, UUID sourceAccountId,
                                        BigDecimal amount, String currency) {
        PaymentInitiatedEvent event = new PaymentInitiatedEvent(
                transactionId,
                sourceAccountId,
                amount,
                currency,
                Instant.now()
        );

        kafkaTemplate.send("payment.initiated", transactionId.toString(), event);
        log.info("Published PaymentInitiatedEvent for transaction: {}", transactionId);
    }

    /**
     * Publica el evento cuando los fondos han sido reservados.
     *
     * Topic: payment.completed
     * Consumidor: ledger-service
     *
     * Solicita a ledger-service que registre los asientos contables
     * (débito a cuenta origen, crédito a cuenta destino).
     */
    public void publishPaymentCompleted(UUID transactionId, UUID sourceAccountId,
                                        UUID destinationAccountId, BigDecimal amount,
                                        String currency) {
        PaymentCompletedEvent event = new PaymentCompletedEvent(
                transactionId,
                sourceAccountId,
                destinationAccountId,
                amount,
                currency,
                Instant.now()
        );

        kafkaTemplate.send("payment.completed", transactionId.toString(), event);
        log.info("Published PaymentCompletedEvent for transaction: {}", transactionId);
    }

    /**
     * Publica el evento de reversión del Saga.
     *
     * Topic: payment.reversed
     * Consumidores: account-service (libera la reserva), ledger-service (registra compensación)
     *
     * Se publica cuando hay un fallo después de haber reservado fondos.
     * Inicia la secuencia de compensación para revertir todos los cambios.
     */
    public void publishPaymentReversed(UUID transactionId, UUID sourceAccountId,
                                       UUID destinationAccountId, BigDecimal amount,
                                       String currency, String reversalReason) {
        PaymentReversedEvent event = new PaymentReversedEvent(
                transactionId,
                sourceAccountId,
                destinationAccountId,
                amount,
                currency,
                reversalReason,
                Instant.now()
        );

        kafkaTemplate.send("payment.reversed", transactionId.toString(), event);
        log.info("Published PaymentReversedEvent for transaction: {} with reason: {}",
                transactionId, reversalReason);
    }

    /**
     * Publica que el pago terminó en fallo.
     *
     * Topic: payment.failed
     * Consumidores: interesados (auditoría, notificaciones)
     *
     * Se publica en: rechazo por fraude, fallo en la reserva de fondos (nada que revertir)
     * y cuando account-service confirma la liberación de fondos tras una compensación.
     */
    public void publishPaymentFailed(UUID transactionId, UUID userId,
                                     UUID sourceAccountId, UUID destinationAccountId,
                                     BigDecimal amount, String currency, String failureReason) {
        PaymentFailedEvent event = new PaymentFailedEvent(
                transactionId,
                userId,
                sourceAccountId,
                destinationAccountId,
                amount,
                currency,
                failureReason,
                Instant.now()
        );

        kafkaTemplate.send("payment.failed", transactionId.toString(), event);
        log.info("Published PaymentFailedEvent for transaction: {} with reason: {}",
                transactionId, failureReason);
    }
}
