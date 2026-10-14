package com.finnode.payment.kafka;

import com.finnode.payment.event.FraudCheckCompletedEvent;
import com.finnode.payment.event.FundsReleasedEvent;
import com.finnode.payment.event.FundsReservationFailedEvent;
import com.finnode.payment.event.FundsReservedEvent;
import com.finnode.payment.event.LedgerEntriesRecordedEvent;
import com.finnode.payment.event.LedgerRecordingFailedEvent;
import com.finnode.payment.service.SagaOrchestrator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * Consumidor de eventos para el Saga de pagos.
 *
 * Escucha los topics de response de otros microservicios:
 *  - fraud.check-completed            → FraudCheckCompletedEvent (fraud-engine)
 *  - account.funds-reserved           → FundsReservedEvent
 *  - account.funds-reservation-failed → FundsReservationFailedEvent
 *  - account.funds-released           → FundsReleasedEvent (confirma la compensación)
 *  - ledger.entries-recorded          → LedgerEntriesRecordedEvent
 *  - ledger.recording-failed          → LedgerRecordingFailedEvent (compensación)
 *
 * Al recibir cada evento, llama a SagaOrchestrator para avanzar, revertir
 * o completar el flujo según corresponda.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentEventConsumer {

    private final SagaOrchestrator sagaOrchestrator;

    /**
     * Escucha la decisión del motor de fraude.
     *
     * Topic: fraud.check-completed
     * Origen: fraud-engine (Python / FastAPI)
     *
     * Transición: PENDING (FRAUD_CHECK) → PENDING (RESERVE_FUNDS) si APPROVED
     *             PENDING (FRAUD_CHECK) → FRAUD_REJECTED si REJECTED
     * Acción: APPROVED → publicar payment.initiated (reserva de fondos);
     *         REJECTED → publicar payment.failed y notificar al usuario
     */
    @KafkaListener(
            topics = "fraud.check-completed",
            groupId = "payment-orchestrator-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onFraudCheckCompleted(FraudCheckCompletedEvent event) {
        log.info("Received FraudCheckCompletedEvent for transaction: {}, decision: {}, riskScore: {}",
                event.transactionId(), event.decision(), event.riskScore());
        try {
            sagaOrchestrator.onFraudCheckCompleted(event);
        } catch (Exception e) {
            log.error("Error processing FraudCheckCompletedEvent: {}", event, e);
            // En producción, enviar a DLQ o reintentarlo
        }
    }

    /**
     * Escucha eventos de reserva de fondos exitosa.
     *
     * Topic: account.funds-reserved
     * Origen: account-service
     *
     * Transición: PENDING → FUNDS_RESERVED
     * Acción: Solicitar a ledger-service que registre asientos
     */
    @KafkaListener(
            topics = "account.funds-reserved",
            groupId = "payment-orchestrator-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onFundsReserved(FundsReservedEvent event) {
        log.info("Received FundsReservedEvent for transaction: {}", event.transactionId());
        try {
            sagaOrchestrator.onFundsReserved(event);
        } catch (Exception e) {
            log.error("Error processing FundsReservedEvent: {}", event, e);
            // En producción, enviar a DLQ o reintentarlo
        }
    }

    /**
     * Escucha eventos de fallo en la reserva de fondos.
     *
     * Topic: account.funds-reservation-failed
     * Origen: account-service
     *
     * Razones: saldo insuficiente, cuenta suspendida, etc.
     * Transición: PENDING → FAILED
     * Acción: Marcar transacción como fallida (sin compensación necesaria)
     */
    @KafkaListener(
            topics = "account.funds-reservation-failed",
            groupId = "payment-orchestrator-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onFundsReservationFailed(FundsReservationFailedEvent event) {
        log.info("Received FundsReservationFailedEvent for transaction: {}", event.transactionId());
        try {
            sagaOrchestrator.onFundsReservationFailed(event);
        } catch (Exception e) {
            log.error("Error processing FundsReservationFailedEvent: {}", event, e);
            // En producción, enviar a DLQ o reintentarlo
        }
    }

    /**
     * Escucha la confirmación de liberación de fondos (fin de la compensación).
     *
     * Topic: account.funds-released
     * Origen: account-service
     *
     * Se publica cuando account-service procesó payment.reversed y liberó la reserva.
     * Estado: REVERSED (sin cambio)
     * Acción: Publicar payment.failed y notificar al usuario que el pago fue revertido
     */
    @KafkaListener(
            topics = "account.funds-released",
            groupId = "payment-orchestrator-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onFundsReleased(FundsReleasedEvent event) {
        log.info("Received FundsReleasedEvent for transaction: {}", event.transactionId());
        try {
            sagaOrchestrator.onFundsReleased(event);
        } catch (Exception e) {
            log.error("Error processing FundsReleasedEvent: {}", event, e);
            // En producción, enviar a DLQ o reintentarlo
        }
    }

    /**
     * Escucha eventos de registros contables exitosos.
     *
     * Topic: ledger.entries-recorded
     * Origen: ledger-service
     *
     * Transición: FUNDS_RESERVED → COMPLETED
     * Acción: Completar el Saga (éxito total)
     */
    @KafkaListener(
            topics = "ledger.entries-recorded",
            groupId = "payment-orchestrator-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onLedgerRecorded(LedgerEntriesRecordedEvent event) {
        log.info("Received LedgerEntriesRecordedEvent for transaction: {}", event.transactionId());
        try {
            sagaOrchestrator.onLedgerRecorded(event);
        } catch (Exception e) {
            log.error("Error processing LedgerEntriesRecordedEvent: {}", event, e);
            // En producción, enviar a DLQ o reintentarlo
        }
    }

    /**
     * Escucha eventos de fallo en el registro contable.
     *
     * Topic: ledger.recording-failed
     * Origen: ledger-service
     *
     * Razones: timeout, error de base de datos, etc.
     * Transición: FUNDS_RESERVED → REVERSED
     * Acción: Iniciar compensación (liberar fondos, registrar asientos inversos)
     */
    @KafkaListener(
            topics = "ledger.recording-failed",
            groupId = "payment-orchestrator-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onLedgerRecordingFailed(LedgerRecordingFailedEvent event) {
        log.warn("Received LedgerRecordingFailedEvent for transaction: {}, reason: {}",
                event.transactionId(), event.reason());
        try {
            sagaOrchestrator.compensateTransaction(event.transactionId().toString(), event.reason());
        } catch (Exception e) {
            log.error("Error processing LedgerRecordingFailedEvent: {}", event, e);
            // En producción, enviar a DLQ o reintentarlo
        }
    }
}