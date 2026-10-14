package com.finnode.payment.service;

import com.finnode.payment.dto.PaymentNotification;
import com.finnode.payment.event.FraudCheckCompletedEvent;
import com.finnode.payment.event.FundsReleasedEvent;
import com.finnode.payment.event.FundsReservationFailedEvent;
import com.finnode.payment.event.FundsReservedEvent;
import com.finnode.payment.event.LedgerEntriesRecordedEvent;
import com.finnode.payment.exception.PaymentProcessingException;
import com.finnode.payment.kafka.PaymentEventPublisher;
import com.finnode.payment.model.SagaStep;
import com.finnode.payment.model.Transaction;
import com.finnode.payment.model.TransactionStatus;
import com.finnode.payment.repository.TransactionRepository;
import com.finnode.payment.websocket.PaymentNotificationHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Orquestador del Patrón Saga para transacciones de pago.
 *
 * Gestiona la máquina de estados del Saga. Es el único componente que conoce
 * el flujo completo de una transacción.
 *
 * Al recibir cada evento de respuesta (FraudCheckCompleted, FundsReserved, LedgerRecorded, etc.),
 * decide si avanzar al siguiente paso o iniciar la secuencia de compensación.
 * Actualiza el TransactionStatus y SagaStep en cada transición.
 *
 * Los eventos de Kafka y las notificaciones WebSocket se emiten DESPUÉS del commit de la
 * transacción de base de datos. Así, una respuesta rápida de otro servicio (ej. fraud-engine)
 * nunca llega antes de que el nuevo estado esté persistido.
 *
 * Transiciones válidas:
 *  - PENDING (FRAUD_CHECK) → FRAUD_REJECTED (terminal, fraude rechazado o sin respuesta)
 *  - PENDING (FRAUD_CHECK) → PENDING (RESERVE_FUNDS)   (fraude aprobado)
 *  - PENDING → FUNDS_RESERVED → COMPLETED (exitoso)
 *  - PENDING → FAILED (sin fondos reservados)
 *  - FUNDS_RESERVED → REVERSED (con compensación)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SagaOrchestrator {

    private static final int MAX_FAILURE_REASON_LENGTH = 500;

    private final TransactionRepository transactionRepository;
    private final PaymentEventPublisher paymentEventPublisher;
    private final PaymentNotificationHandler paymentNotificationHandler;

    /**
     * Primer paso del Saga: solicita la evaluación de fraude.
     *
     * Estado: PENDING, paso FRAUD_CHECK (la transacción ya fue persistida por PaymentService).
     * Acción: publica fraud.check-requested. La respuesta llega por fraud.check-completed
     * y se procesa en {@link #onFraudCheckCompleted(FraudCheckCompletedEvent)}.
     *
     * No se reserva ni se toca ningún saldo hasta que fraud-engine apruebe.
     */
    @Transactional
    public void requestFraudCheck(Transaction transaction) {
        log.info("Requesting fraud check for transaction {}", transaction.getTransactionId());

        final UUID transactionId = transaction.getTransactionId();
        final UUID userId = transaction.getUserId();
        final UUID sourceAccountId = transaction.getSourceAccountId();
        final UUID destinationAccountId = transaction.getDestinationAccountId();
        final var amount = transaction.getAmount();
        final String currency = transaction.getCurrency();

        afterCommit(() -> paymentEventPublisher.publishFraudCheckRequested(
                transactionId, userId, sourceAccountId, destinationAccountId, amount, currency));
    }

    /**
     * Procesa el evento FraudCheckCompletedEvent (respuesta de fraud-engine).
     *
     * Transición: PENDING (FRAUD_CHECK) → PENDING (RESERVE_FUNDS) si APPROVED
     *             PENDING (FRAUD_CHECK) → FRAUD_REJECTED si REJECTED
     *
     * Si la transacción ya no está esperando fraude (respuesta duplicada o tardía
     * después de un timeout), el evento se ignora.
     */
    @Transactional
    public void onFraudCheckCompleted(FraudCheckCompletedEvent event) {
        log.info("Processing FraudCheckCompletedEvent for transaction: {}", event.transactionId());

        Transaction transaction = findTransaction(event.transactionId());

        if (transaction.getStatus() != TransactionStatus.PENDING
                || transaction.getCurrentStep() != SagaStep.FRAUD_CHECK) {
            log.warn("Ignoring FraudCheckCompletedEvent for transaction {} in status {} / step {}",
                    event.transactionId(), transaction.getStatus(), transaction.getCurrentStep());
            return;
        }

        double riskScore = event.riskScore() == null ? 0.0 : event.riskScore().doubleValue();

        if (event.isApproved()) {
            transaction.setFraudRiskScore(riskScore);
            log.info("Transaction {} approved by fraud-engine (riskScore: {})",
                    event.transactionId(), riskScore);
            startSaga(transaction);
        } else {
            // Cualquier decisión distinta de APPROVED se rechaza (fail-closed)
            rejectAsFraud(transaction, riskScore, buildFraudReason(event));
        }
    }

    /**
     * Procesa el evento FundsReservedEvent.
     *
     * Transición: PENDING → FUNDS_RESERVED
     * Acción: Solicita a ledger-service que registre los asientos contables
     */
    @Transactional
    public void onFundsReserved(FundsReservedEvent event) {
        log.info("Processing FundsReservedEvent for transaction: {}", event.transactionId());

        Transaction transaction = findTransaction(event.transactionId());

        if (transaction.getStatus() != TransactionStatus.PENDING
                || transaction.getCurrentStep() != SagaStep.RESERVE_FUNDS) {
            log.warn("Ignoring FundsReservedEvent for transaction {} in status {} / step {}",
                    event.transactionId(), transaction.getStatus(), transaction.getCurrentStep());
            return;
        }

        // Actualizar estado: fondos reservados
        transaction.setStatus(TransactionStatus.FUNDS_RESERVED);
        transaction.setCurrentStep(SagaStep.RECORD_LEDGER);
        transactionRepository.save(transaction);

        log.info("Transaction {} advanced to FUNDS_RESERVED state", event.transactionId());

        // Publicar evento para que ledger-service registre los asientos
        final UUID transactionId = event.transactionId();
        final UUID sourceAccountId = transaction.getSourceAccountId();
        final UUID destinationAccountId = transaction.getDestinationAccountId();
        final var amount = event.amount();
        final String currency = transaction.getCurrency();

        afterCommit(() -> paymentEventPublisher.publishPaymentCompleted(
                transactionId, sourceAccountId, destinationAccountId, amount, currency));
    }

    /**
     * Procesa el evento FundsReservationFailedEvent.
     *
     * Transición: PENDING → FAILED
     *
     * Los fondos nunca fueron reservados, así que no hay nada que compensar.
     * La transacción termina en estado FAILED.
     */
    @Transactional
    public void onFundsReservationFailed(FundsReservationFailedEvent event) {
        log.info("Processing FundsReservationFailedEvent for transaction: {}", event.transactionId());

        Transaction transaction = findTransaction(event.transactionId());

        if (transaction.getStatus() != TransactionStatus.PENDING
                || transaction.getCurrentStep() != SagaStep.RESERVE_FUNDS) {
            log.warn("Ignoring FundsReservationFailedEvent for transaction {} in status {} / step {}",
                    event.transactionId(), transaction.getStatus(), transaction.getCurrentStep());
            return;
        }

        // Actualizar estado: falló la reserva de fondos
        transaction.setStatus(TransactionStatus.FAILED);
        transaction.setCurrentStep(SagaStep.COMPENSATE);
        transaction.setFailureReason(event.reason());
        transactionRepository.save(transaction);

        log.warn("Transaction {} failed at funds reservation: {}",
                event.transactionId(), event.reason());

        // No publicamos payment.reversed porque nunca hubo fondos reservados.
        // Sí informamos: payment.failed + notificación WebSocket al usuario.
        publishFailureAndNotify(transaction, event.reason(),
                "No se pudo realizar la transferencia: " + event.reason());
    }

    /**
     * Procesa el evento LedgerEntriesRecordedEvent.
     *
     * Transición: FUNDS_RESERVED → COMPLETED
     *
     * Los asientos contables han sido registrados exitosamente.
     * El Saga ha completado todas sus fases.
     */
    @Transactional
    public void onLedgerRecorded(LedgerEntriesRecordedEvent event) {
        log.info("Processing LedgerEntriesRecordedEvent for transaction: {}", event.transactionId());

        Transaction transaction = findTransaction(event.transactionId());

        if (transaction.getStatus() != TransactionStatus.FUNDS_RESERVED) {
            log.warn("Ignoring LedgerEntriesRecordedEvent for transaction {} in status {}",
                    event.transactionId(), transaction.getStatus());
            return;
        }

        // Actualizar estado: transacción completada
        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setCurrentStep(SagaStep.CONFIRM_PAYMENT);
        transactionRepository.save(transaction);

        log.info("Transaction {} successfully completed", event.transactionId());

        notifyUser(transaction, "Transferencia completada exitosamente");
    }

    /**
     * Procesa el evento LedgerRecordingFailedEvent (si los ledger no registran).
     *
     * Transición: FUNDS_RESERVED → REVERSED
     *
     * Los fondos ya fueron reservados, pero el registro contable falló.
     * Iniciamos compensación: liberamos la reserva de fondos y registramos
     * asientos de compensación.
     *
     * El usuario se notifica cuando account-service confirma la liberación
     * (ver {@link #onFundsReleased(FundsReleasedEvent)}).
     */
    @Transactional
    public void compensateTransaction(String transactionId, String reason) {
        log.warn("Initiating compensation for transaction {} due to: {}", transactionId, reason);

        Transaction transaction = findTransaction(UUID.fromString(transactionId));

        if (transaction.getStatus() != TransactionStatus.FUNDS_RESERVED) {
            log.warn("Cannot compensate transaction {} in status {}. No funds were reserved.",
                    transactionId, transaction.getStatus());
            return;
        }

        // Actualizar estado: transacción revertida
        transaction.setStatus(TransactionStatus.REVERSED);
        transaction.setCurrentStep(SagaStep.COMPENSATE);
        transaction.setFailureReason(reason);
        transactionRepository.save(transaction);

        log.info("Transaction {} marked as REVERSED", transactionId);

        // Publicar evento de reversión para que account-service y ledger-service
        // realicen las operaciones de compensación
        final UUID txId = transaction.getTransactionId();
        final UUID sourceAccountId = transaction.getSourceAccountId();
        final UUID destinationAccountId = transaction.getDestinationAccountId();
        final var amount = transaction.getAmount();
        final String currency = transaction.getCurrency();

        afterCommit(() -> paymentEventPublisher.publishPaymentReversed(
                txId, sourceAccountId, destinationAccountId, amount, currency, reason));
    }

    /**
     * Procesa el evento FundsReleasedEvent (account-service liberó la reserva).
     *
     * Estado: REVERSED (sin cambio) — confirma que la compensación terminó.
     * Acción: publica payment.failed y notifica al usuario por WebSocket.
     */
    @Transactional
    public void onFundsReleased(FundsReleasedEvent event) {
        log.info("Processing FundsReleasedEvent for transaction: {}", event.transactionId());

        Transaction transaction = findTransaction(event.transactionId());

        if (transaction.getStatus() != TransactionStatus.REVERSED) {
            log.warn("Ignoring FundsReleasedEvent for transaction {} in status {}",
                    event.transactionId(), transaction.getStatus());
            return;
        }

        log.info("Compensation confirmed for transaction {}", event.transactionId());

        publishFailureAndNotify(transaction, transaction.getFailureReason(),
                "La transferencia fue revertida y tus fondos fueron liberados");
    }

    /**
     * Rechaza una transacción detectada como fraude.
     *
     * Transición: PENDING → FRAUD_REJECTED
     *
     * El Saga nunca reserva fondos, por lo que no hay compensación necesaria.
     * Publica payment.failed y notifica al usuario por WebSocket.
     */
    @Transactional
    public void rejectAsFraud(Transaction transaction, double riskScore, String reason) {
        log.warn("Rejecting transaction {} as fraud. Risk score: {}",
                transaction.getTransactionId(), riskScore);

        transaction.setStatus(TransactionStatus.FRAUD_REJECTED);
        transaction.setCurrentStep(SagaStep.FRAUD_CHECK);
        transaction.setFraudRiskScore(riskScore);
        transaction.setFailureReason(reason);
        transactionRepository.save(transaction);

        log.info("Transaction {} rejected as fraud", transaction.getTransactionId());

        publishFailureAndNotify(transaction, reason,
                "Tu transferencia fue rechazada por seguridad");
    }

    /**
     * Continúa el Saga después de que fraud-engine aprobó la transacción.
     *
     * Transición: PENDING (FRAUD_CHECK) → PENDING (RESERVE_FUNDS)
     *
     * Publica el evento PaymentInitiatedEvent para que account-service
     * inicie la reserva de fondos.
     */
    @Transactional
    public void startSaga(Transaction transaction) {
        log.info("Starting Saga for transaction {}", transaction.getTransactionId());

        transaction.setStatus(TransactionStatus.PENDING);
        transaction.setCurrentStep(SagaStep.RESERVE_FUNDS);
        transactionRepository.save(transaction);

        // Publicar evento para que account-service reserve los fondos
        final UUID transactionId = transaction.getTransactionId();
        final UUID sourceAccountId = transaction.getSourceAccountId();
        final var amount = transaction.getAmount();
        final String currency = transaction.getCurrency();

        afterCommit(() -> {
            paymentEventPublisher.publishPaymentInitiated(transactionId, sourceAccountId, amount, currency);
            log.info("PaymentInitiatedEvent published for transaction {}", transactionId);
        });
    }

    // ───────────────────────── utilidades ─────────────────────────

    private Transaction findTransaction(UUID transactionId) {
        return transactionRepository
                .findByTransactionId(transactionId)
                .orElseThrow(() -> new PaymentProcessingException(
                        "Transaction not found: " + transactionId));
    }

    /** Motivo del rechazo: FRAUD_REJECTED + códigos de reglas activadas (máx. 500 caracteres, longitud de la columna). */
    private String buildFraudReason(FraudCheckCompletedEvent event) {
        String reason = "FRAUD_REJECTED";
        if (event.triggeredRules() != null && !event.triggeredRules().isEmpty()) {
            String codes = event.triggeredRules().stream()
                    .map(FraudCheckCompletedEvent.TriggeredRule::code)
                    .collect(Collectors.joining(","));
            reason = reason + ":" + codes;
        }
        return reason.length() > MAX_FAILURE_REASON_LENGTH
                ? reason.substring(0, MAX_FAILURE_REASON_LENGTH)
                : reason;
    }

    /** Publica payment.failed y notifica por WebSocket, ambos después del commit. */
    private void publishFailureAndNotify(Transaction transaction, String failureReason, String message) {
        final UUID transactionId = transaction.getTransactionId();
        final UUID userId = transaction.getUserId();
        final UUID sourceAccountId = transaction.getSourceAccountId();
        final UUID destinationAccountId = transaction.getDestinationAccountId();
        final var amount = transaction.getAmount();
        final String currency = transaction.getCurrency();
        final PaymentNotification notification = PaymentNotification.of(
                transactionId, transaction.getStatus(), failureReason, message);

        afterCommit(() -> {
            paymentEventPublisher.publishPaymentFailed(
                    transactionId, userId, sourceAccountId, destinationAccountId,
                    amount, currency, failureReason);
            paymentNotificationHandler.notifyUser(userId, notification);
        });
    }

    /** Notifica por WebSocket al dueño de la transacción, después del commit. */
    private void notifyUser(Transaction transaction, String message) {
        final UUID userId = transaction.getUserId();
        final PaymentNotification notification = PaymentNotification.of(
                transaction.getTransactionId(), transaction.getStatus(),
                transaction.getFailureReason(), message);

        afterCommit(() -> paymentNotificationHandler.notifyUser(userId, notification));
    }

    /**
     * Ejecuta la acción cuando la transacción de BD haya hecho commit.
     * Si no hay transacción activa, la ejecuta de inmediato.
     */
    private void afterCommit(Runnable action) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    runSafely(action);
                }
            });
        } else {
            runSafely(action);
        }
    }

    private void runSafely(Runnable action) {
        try {
            action.run();
        } catch (Exception e) {
            // El estado ya está persistido; un fallo al publicar/notificar no debe revertirlo.
            log.error("Error executing post-commit action: {}", e.getMessage(), e);
        }
    }
}