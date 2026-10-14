package com.finnode.payment.service;

import com.finnode.payment.event.FraudCheckCompletedEvent;
import com.finnode.payment.event.FraudCheckCompletedEvent.Decision;
import com.finnode.payment.event.FundsReleasedEvent;
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
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Flujo de fraude asíncrono (fraud.check-requested / fraud.check-completed) y confirmación de
 * compensación (account.funds-released).
 *
 * Firmas reales (confirmadas por el compilador):
 *   - FraudCheckCompletedEvent(UUID transactionId, FraudCheckCompletedEvent.Decision decision,
 *       Integer riskScore, List<FraudCheckCompletedEvent.TriggeredRule> triggeredRules, Instant evaluatedAt)
 *   - FundsReleasedEvent(UUID transactionId, String accountId, BigDecimal amount, Instant timestamp)
 * La decisión es un enum, por lo que un valor desconocido en el JSON se rechaza al deserializar
 * (no llega al orquestador); aquí solo se prueba la decisión nula, que sí puede llegar.
 * Si algún constructor cambia, solo hay que ajustar {@code fraudEvent(...)} y {@code releasedEvent()}.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("SagaOrchestrator — flujo de fraude y compensación")
class SagaOrchestratorFraudFlowTest {

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
                .amount(new BigDecimal("500000.00"))
                .currency("COP")
                .status(TransactionStatus.PENDING)
                .currentStep(SagaStep.FRAUD_CHECK)
                .build();
    }

    @AfterEach
    void tearDown() {
        TransactionSynchronizationManager.clearSynchronization();
    }

    private void commit() {
        new ArrayList<>(TransactionSynchronizationManager.getSynchronizations())
                .forEach(TransactionSynchronization::afterCommit);
    }

    // ---------------------------------------------------------------
    // onFraudCheckCompleted
    // ---------------------------------------------------------------

    @Test
    @DisplayName("APPROVED: guarda el score, pasa a RESERVE_FUNDS y publica payment.initiated tras el commit")
    void onFraudCheckCompleted_whenApproved_shouldStartSaga() {
        when(transactionRepository.findByTransactionId(transactionId)).thenReturn(Optional.of(transaction));

        sagaOrchestrator.onFraudCheckCompleted(fraudEvent(Decision.APPROVED, 30));
        commit();

        assertThat(transaction.getFraudRiskScore()).isEqualTo(30.0);
        assertThat(transaction.getStatus()).isEqualTo(TransactionStatus.PENDING);
        assertThat(transaction.getCurrentStep()).isEqualTo(SagaStep.RESERVE_FUNDS);

        verify(paymentEventPublisher).publishPaymentInitiated(
                transactionId,
                transaction.getSourceAccountId(),
                transaction.getAmount(),
                transaction.getCurrency());
    }

    @Test
    @DisplayName("REJECTED: FRAUD_REJECTED, guarda score y motivo FRAUD_REJECTED..., y NO reserva fondos")
    void onFraudCheckCompleted_whenRejected_shouldRejectWithoutReservingFunds() {
        when(transactionRepository.findByTransactionId(transactionId)).thenReturn(Optional.of(transaction));

        sagaOrchestrator.onFraudCheckCompleted(fraudEvent(Decision.REJECTED, 85));
        commit();

        assertThat(transaction.getStatus()).isEqualTo(TransactionStatus.FRAUD_REJECTED);
        assertThat(transaction.getCurrentStep()).isEqualTo(SagaStep.FRAUD_CHECK);
        assertThat(transaction.getFraudRiskScore()).isEqualTo(85.0);
        assertThat(transaction.getFailureReason()).startsWith("FRAUD_REJECTED");
        verify(transactionRepository).save(transaction);
        verify(paymentEventPublisher, never()).publishPaymentInitiated(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Decisión nula: se trata como rechazo (fail-closed)")
    void onFraudCheckCompleted_whenNullDecision_shouldReject() {
        when(transactionRepository.findByTransactionId(transactionId)).thenReturn(Optional.of(transaction));

        sagaOrchestrator.onFraudCheckCompleted(fraudEvent(null, null));
        commit();

        assertThat(transaction.getStatus()).isEqualTo(TransactionStatus.FRAUD_REJECTED);
        verify(paymentEventPublisher, never()).publishPaymentInitiated(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Respuesta tardía o duplicada (ya no está en PENDING + FRAUD_CHECK): se ignora")
    void onFraudCheckCompleted_whenNotWaitingForFraud_shouldBeIgnored() {
        transaction.setCurrentStep(SagaStep.RESERVE_FUNDS);
        when(transactionRepository.findByTransactionId(transactionId)).thenReturn(Optional.of(transaction));

        sagaOrchestrator.onFraudCheckCompleted(fraudEvent(Decision.REJECTED, 99));
        commit();

        assertThat(transaction.getStatus()).isEqualTo(TransactionStatus.PENDING);
        assertThat(transaction.getCurrentStep()).isEqualTo(SagaStep.RESERVE_FUNDS);
        verify(transactionRepository, never()).save(any());
        verifyNoInteractions(paymentEventPublisher);
    }

    // ---------------------------------------------------------------
    // onFundsReleased
    // ---------------------------------------------------------------

    @Test
    @DisplayName("funds-released en REVERSED: confirma la compensación sin cambiar el estado")
    void onFundsReleased_whenReversed_shouldNotChangeState() {
        transaction.setStatus(TransactionStatus.REVERSED);
        transaction.setCurrentStep(SagaStep.COMPENSATE);
        when(transactionRepository.findByTransactionId(transactionId)).thenReturn(Optional.of(transaction));

        sagaOrchestrator.onFundsReleased(releasedEvent());
        commit();

        assertThat(transaction.getStatus()).isEqualTo(TransactionStatus.REVERSED);
        assertThat(transaction.getCurrentStep()).isEqualTo(SagaStep.COMPENSATE);
        verify(transactionRepository, never()).save(any());
    }

    @Test
    @DisplayName("funds-released cuando la transacción no está REVERSED: se ignora")
    void onFundsReleased_whenNotReversed_shouldBeIgnored() {
        transaction.setStatus(TransactionStatus.COMPLETED);
        when(transactionRepository.findByTransactionId(transactionId)).thenReturn(Optional.of(transaction));

        sagaOrchestrator.onFundsReleased(releasedEvent());
        commit();

        assertThat(transaction.getStatus()).isEqualTo(TransactionStatus.COMPLETED);
        verifyNoInteractions(paymentEventPublisher);
    }

    // ---------------------------------------------------------------
    // Fábricas de eventos (único lugar a ajustar si cambian los constructores)
    // ---------------------------------------------------------------

    private FraudCheckCompletedEvent fraudEvent(Decision decision, Integer riskScore) {
        return new FraudCheckCompletedEvent(transactionId, decision, riskScore, List.of(), Instant.now());
    }

    private FundsReleasedEvent releasedEvent() {
        return new FundsReleasedEvent(transactionId, transaction.getSourceAccountId().toString(),
                transaction.getAmount(), Instant.now());
    }
}