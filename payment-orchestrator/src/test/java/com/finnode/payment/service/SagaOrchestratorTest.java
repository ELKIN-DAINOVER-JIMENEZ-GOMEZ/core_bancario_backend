package com.finnode.payment.service;

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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Pruebas unitarias de {@link SagaOrchestrator}: transiciones que usan eventos con contrato
 * conocido (reserva de fondos, ledger y compensación). El flujo de fraude y
 * account.funds-released está en {@link SagaOrchestratorFraudFlowTest}.
 *
 * Los eventos de Kafka y WebSocket se emiten DESPUÉS del commit. Para simularlo sin base de datos
 * se activa la sincronización de transacciones de Spring en cada test y {@link #commit()}
 * dispara los afterCommit registrados. Si la implementación ejecuta la acción de inmediato,
 * commit() simplemente no encuentra nada que disparar.
 *
 * Campos reales de los eventos (com.finnode.payment.event):
 *   - FundsReservedEvent(UUID transactionId, UUID accountId, BigDecimal amount, Instant timestamp)
 *   - FundsReservationFailedEvent(UUID transactionId, UUID accountId, String reason, Instant timestamp)
 *   - LedgerEntriesRecordedEvent(UUID transactionId, UUID debitEntryId, UUID creditEntryId,
 *       BigDecimal amount, String currency, boolean reversalConfirmed, Instant timestamp)
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("SagaOrchestrator")
class SagaOrchestratorTest {

    @Mock
    private TransactionRepository transactionRepository;

    @Mock
    private PaymentEventPublisher paymentEventPublisher;

    @Mock
    private PaymentNotificationHandler notificationHandler;

    @InjectMocks
    private SagaOrchestrator sagaOrchestrator;

    private Transaction transaction;
    private UUID transactionId;

    @BeforeEach
    void setUp() {
        TransactionSynchronizationManager.initSynchronization();

        transactionId = UUID.randomUUID();
        transaction = Transaction.builder()
                .transactionId(transactionId)
                .userId(UUID.randomUUID())
                .sourceAccountId(UUID.randomUUID())
                .destinationAccountId(UUID.randomUUID())
                .amount(new BigDecimal("1000.00"))
                .currency("COP")
                .status(TransactionStatus.PENDING)
                .currentStep(SagaStep.RESERVE_FUNDS)
                .build();
    }

    @AfterEach
    void tearDown() {
        TransactionSynchronizationManager.clearSynchronization();
    }

    /** Simula el commit de la transacción: ejecuta los afterCommit registrados. */
    private void commit() {
        new ArrayList<>(TransactionSynchronizationManager.getSynchronizations())
                .forEach(TransactionSynchronization::afterCommit);
    }

    private LedgerEntriesRecordedEvent ledgerEvent() {
        return new LedgerEntriesRecordedEvent(
                transactionId, UUID.randomUUID(), UUID.randomUUID(),
                transaction.getAmount(), transaction.getCurrency(), false, Instant.now());
    }

    // ---------------------------------------------------------------
    // startSaga (fraude aprobado → pedir reserva de fondos)
    // ---------------------------------------------------------------

    @Test
    @DisplayName("startSaga: fija PENDING/RESERVE_FUNDS y publica PaymentInitiatedEvent tras el commit")
    void startSaga_shouldSetStepAndPublishInitiatedEventAfterCommit() {
        transaction.setCurrentStep(SagaStep.FRAUD_CHECK);

        sagaOrchestrator.startSaga(transaction);

        assertThat(transaction.getStatus()).isEqualTo(TransactionStatus.PENDING);
        assertThat(transaction.getCurrentStep()).isEqualTo(SagaStep.RESERVE_FUNDS);
        verify(transactionRepository).save(transaction);

        // Aún no hubo commit: no debe haberse publicado nada
        verifyNoInteractions(paymentEventPublisher);

        commit();

        verify(paymentEventPublisher).publishPaymentInitiated(
                transaction.getTransactionId(),
                transaction.getSourceAccountId(),
                transaction.getAmount(),
                transaction.getCurrency()
        );
    }

    // ---------------------------------------------------------------
    // onFundsReserved
    // ---------------------------------------------------------------

    @Test
    @DisplayName("onFundsReserved: PENDING/RESERVE_FUNDS → FUNDS_RESERVED/RECORD_LEDGER y publica payment.completed")
    void onFundsReserved_whenPending_shouldAdvanceAndPublishAfterCommit() {
        when(transactionRepository.findByTransactionId(transactionId)).thenReturn(Optional.of(transaction));

        sagaOrchestrator.onFundsReserved(new FundsReservedEvent(
                transactionId, transaction.getSourceAccountId(), transaction.getAmount(), Instant.now()));

        assertThat(transaction.getStatus()).isEqualTo(TransactionStatus.FUNDS_RESERVED);
        assertThat(transaction.getCurrentStep()).isEqualTo(SagaStep.RECORD_LEDGER);
        verify(transactionRepository).save(transaction);
        verifyNoInteractions(paymentEventPublisher);

        commit();

        verify(paymentEventPublisher).publishPaymentCompleted(
                transactionId,
                transaction.getSourceAccountId(),
                transaction.getDestinationAccountId(),
                transaction.getAmount(),
                transaction.getCurrency()
        );
    }

    @Test
    @DisplayName("onFundsReserved: se ignora si la transacción ya no está en PENDING (idempotencia)")
    void onFundsReserved_whenNotPending_shouldBeIgnored() {
        transaction.setStatus(TransactionStatus.FUNDS_RESERVED);
        when(transactionRepository.findByTransactionId(transactionId)).thenReturn(Optional.of(transaction));

        sagaOrchestrator.onFundsReserved(new FundsReservedEvent(
                transactionId, transaction.getSourceAccountId(), transaction.getAmount(), Instant.now()));
        commit();

        verify(transactionRepository, never()).save(any());
        verifyNoInteractions(paymentEventPublisher);
    }

    @Test
    @DisplayName("onFundsReserved: se ignora si el paso no es RESERVE_FUNDS (p. ej. aún en FRAUD_CHECK)")
    void onFundsReserved_whenStepIsFraudCheck_shouldBeIgnored() {
        transaction.setCurrentStep(SagaStep.FRAUD_CHECK);
        when(transactionRepository.findByTransactionId(transactionId)).thenReturn(Optional.of(transaction));

        sagaOrchestrator.onFundsReserved(new FundsReservedEvent(
                transactionId, transaction.getSourceAccountId(), transaction.getAmount(), Instant.now()));
        commit();

        assertThat(transaction.getStatus()).isEqualTo(TransactionStatus.PENDING);
        verify(transactionRepository, never()).save(any());
        verifyNoInteractions(paymentEventPublisher);
    }

    @Test
    @DisplayName("onFundsReserved: lanza PaymentProcessingException si la transacción no existe")
    void onFundsReserved_whenTransactionNotFound_shouldThrow() {
        when(transactionRepository.findByTransactionId(transactionId)).thenReturn(Optional.empty());

        FundsReservedEvent event = new FundsReservedEvent(
                transactionId, UUID.randomUUID(), new BigDecimal("1000.00"), Instant.now());

        assertThatThrownBy(() -> sagaOrchestrator.onFundsReserved(event))
                .isInstanceOf(PaymentProcessingException.class);

        verifyNoInteractions(paymentEventPublisher);
    }

    // ---------------------------------------------------------------
    // onFundsReservationFailed
    // ---------------------------------------------------------------

    @Test
    @DisplayName("onFundsReservationFailed: PENDING → FAILED/COMPENSATE con el motivo, sin publicar compensación")
    void onFundsReservationFailed_whenPending_shouldMarkFailed() {
        when(transactionRepository.findByTransactionId(transactionId)).thenReturn(Optional.of(transaction));

        sagaOrchestrator.onFundsReservationFailed(new FundsReservationFailedEvent(
                transactionId, transaction.getSourceAccountId(), "Saldo insuficiente", Instant.now()));
        commit();

        assertThat(transaction.getStatus()).isEqualTo(TransactionStatus.FAILED);
        assertThat(transaction.getCurrentStep()).isEqualTo(SagaStep.COMPENSATE);
        assertThat(transaction.getFailureReason()).isEqualTo("Saldo insuficiente");
        verify(transactionRepository).save(transaction);

        // Nada que revertir: no se completa ni se revierte; solo se informa (payment.failed)
        verify(paymentEventPublisher, never()).publishPaymentInitiated(any(), any(), any(), any());
        verify(paymentEventPublisher, never()).publishPaymentCompleted(any(), any(), any(), any(), any());
        verify(paymentEventPublisher, never()).publishPaymentReversed(any(), any(), any(), any(), any(), any());
    }

    @Test
    @DisplayName("onFundsReservationFailed: se ignora si la transacción no está en PENDING")
    void onFundsReservationFailed_whenNotPending_shouldBeIgnored() {
        transaction.setStatus(TransactionStatus.COMPLETED);
        when(transactionRepository.findByTransactionId(transactionId)).thenReturn(Optional.of(transaction));

        sagaOrchestrator.onFundsReservationFailed(new FundsReservationFailedEvent(
                transactionId, transaction.getSourceAccountId(), "irrelevante", Instant.now()));
        commit();

        assertThat(transaction.getStatus()).isEqualTo(TransactionStatus.COMPLETED);
        verify(transactionRepository, never()).save(any());
    }

    // ---------------------------------------------------------------
    // onLedgerRecorded
    // ---------------------------------------------------------------

    @Test
    @DisplayName("onLedgerRecorded: FUNDS_RESERVED → COMPLETED/CONFIRM_PAYMENT")
    void onLedgerRecorded_whenFundsReserved_shouldComplete() {
        transaction.setStatus(TransactionStatus.FUNDS_RESERVED);
        transaction.setCurrentStep(SagaStep.RECORD_LEDGER);
        when(transactionRepository.findByTransactionId(transactionId)).thenReturn(Optional.of(transaction));

        sagaOrchestrator.onLedgerRecorded(ledgerEvent());
        commit();

        assertThat(transaction.getStatus()).isEqualTo(TransactionStatus.COMPLETED);
        assertThat(transaction.getCurrentStep()).isEqualTo(SagaStep.CONFIRM_PAYMENT);
        verify(transactionRepository).save(transaction);
    }

    @Test
    @DisplayName("onLedgerRecorded: se ignora si la transacción no está en FUNDS_RESERVED")
    void onLedgerRecorded_whenNotFundsReserved_shouldBeIgnored() {
        transaction.setStatus(TransactionStatus.PENDING);
        when(transactionRepository.findByTransactionId(transactionId)).thenReturn(Optional.of(transaction));

        sagaOrchestrator.onLedgerRecorded(ledgerEvent());
        commit();

        assertThat(transaction.getStatus()).isEqualTo(TransactionStatus.PENDING);
        verify(transactionRepository, never()).save(any());
    }

    // ---------------------------------------------------------------
    // compensateTransaction
    // ---------------------------------------------------------------

    @Test
    @DisplayName("compensateTransaction: FUNDS_RESERVED → REVERSED/COMPENSATE y publica payment.reversed tras el commit")
    void compensateTransaction_whenFundsReserved_shouldReverseAndPublishAfterCommit() {
        transaction.setStatus(TransactionStatus.FUNDS_RESERVED);
        transaction.setCurrentStep(SagaStep.RECORD_LEDGER);
        when(transactionRepository.findByTransactionId(transactionId)).thenReturn(Optional.of(transaction));

        sagaOrchestrator.compensateTransaction(transactionId.toString(), "Timeout en ledger-service");

        assertThat(transaction.getStatus()).isEqualTo(TransactionStatus.REVERSED);
        assertThat(transaction.getCurrentStep()).isEqualTo(SagaStep.COMPENSATE);
        assertThat(transaction.getFailureReason()).isEqualTo("Timeout en ledger-service");
        verify(transactionRepository).save(transaction);
        verifyNoInteractions(paymentEventPublisher);

        commit();

        verify(paymentEventPublisher).publishPaymentReversed(
                transactionId,
                transaction.getSourceAccountId(),
                transaction.getDestinationAccountId(),
                transaction.getAmount(),
                transaction.getCurrency(),
                "Timeout en ledger-service"
        );
    }

    @Test
    @DisplayName("compensateTransaction: no compensa (ni publica) si los fondos nunca fueron reservados")
    void compensateTransaction_whenNotFundsReserved_shouldDoNothing() {
        transaction.setStatus(TransactionStatus.PENDING);
        when(transactionRepository.findByTransactionId(transactionId)).thenReturn(Optional.of(transaction));

        sagaOrchestrator.compensateTransaction(transactionId.toString(), "cualquier razón");
        commit();

        assertThat(transaction.getStatus()).isEqualTo(TransactionStatus.PENDING);
        verify(transactionRepository, never()).save(any());
        verifyNoInteractions(paymentEventPublisher);
    }

    @Test
    @DisplayName("compensateTransaction: una segunda compensación (ya REVERSED) no vuelve a publicar")
    void compensateTransaction_whenAlreadyReversed_shouldNotPublishAgain() {
        transaction.setStatus(TransactionStatus.REVERSED);
        transaction.setCurrentStep(SagaStep.COMPENSATE);
        when(transactionRepository.findByTransactionId(transactionId)).thenReturn(Optional.of(transaction));

        sagaOrchestrator.compensateTransaction(transactionId.toString(), "duplicado");
        commit();

        verify(transactionRepository, never()).save(any());
        verifyNoInteractions(paymentEventPublisher);
    }

    @Test
    @DisplayName("compensateTransaction: lanza PaymentProcessingException si la transacción no existe")
    void compensateTransaction_whenTransactionNotFound_shouldThrow() {
        when(transactionRepository.findByTransactionId(transactionId)).thenReturn(Optional.empty());

        assertThatThrownBy(() ->
                sagaOrchestrator.compensateTransaction(transactionId.toString(), "razón"))
                .isInstanceOf(PaymentProcessingException.class);
    }
}