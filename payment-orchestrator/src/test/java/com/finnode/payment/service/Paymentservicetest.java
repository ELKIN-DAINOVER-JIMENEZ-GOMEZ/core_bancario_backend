package com.finnode.payment.service;

import com.finnode.payment.dto.TransactionStatusResponse;
import com.finnode.payment.dto.TransferRequest;
import com.finnode.payment.dto.TransferResponse;
import com.finnode.payment.exception.DuplicateTransactionException;
import com.finnode.payment.exception.TransactionNotFoundException;
import com.finnode.payment.model.SagaStep;
import com.finnode.payment.model.Transaction;
import com.finnode.payment.model.TransactionStatus;
import com.finnode.payment.repository.TransactionRepository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Pruebas unitarias de {@link PaymentService}.
 *
 * El fraude ya NO se evalúa aquí (lo hace fraud-engine por Kafka): el servicio solo valida,
 * aplica idempotencia, persiste la Transaction (PENDING / FRAUD_CHECK) y delega en
 * SagaOrchestrator.requestFraudCheck(...).
 *
 * @InjectMocks construye el servicio con los mocks por tipo, así no depende del orden
 * de los parámetros del constructor.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("PaymentService")
class PaymentServiceTest {

    @Mock
    private TransactionRepository transactionRepository;

    @Mock
    private SagaOrchestrator sagaOrchestrator;

    @InjectMocks
    private PaymentService paymentService;

    private UUID userId;
    private UUID sourceAccountId;
    private UUID destinationAccountId;
    private TransferRequest validRequest;

    @BeforeEach
    void setUp() {
        userId = UUID.randomUUID();
        sourceAccountId = UUID.randomUUID();
        destinationAccountId = UUID.randomUUID();

        validRequest = new TransferRequest(
                sourceAccountId.toString(),
                destinationAccountId.toString(),
                new BigDecimal("100000.00"),
                "COP",
                "Pago de arriendo"
        );
    }

    private Transaction existingTransaction(String idempotencyKey) {
        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(UUID.randomUUID())
                .userId(userId)
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destinationAccountId)
                .amount(new BigDecimal("100000.00"))
                .currency("COP")
                .status(TransactionStatus.PENDING)
                .currentStep(SagaStep.FRAUD_CHECK)
                .build();
        tx.setIdempotencyKey(idempotencyKey);
        return tx;
    }

    // ---------------------------------------------------------------
    // initiateTransfer
    // ---------------------------------------------------------------

    @Test
    @DisplayName("Debe persistir la transacción PENDING/FRAUD_CHECK y pedir la evaluación de fraude")
    void initiateTransfer_shouldPersistAndRequestFraudCheck() {
        when(transactionRepository.save(any(Transaction.class))).thenAnswer(inv -> inv.getArgument(0));

        TransferResponse response = paymentService.initiateTransfer(validRequest, userId, null);

        assertThat(response).isNotNull();
        assertThat(response.status()).isEqualTo(TransactionStatus.PENDING);
        assertThat(response.amount()).isEqualByComparingTo(validRequest.amount());
        assertThat(response.currency()).isEqualTo(validRequest.currency());
        assertThat(response.transactionId()).isNotBlank();

        ArgumentCaptor<Transaction> txCaptor = ArgumentCaptor.forClass(Transaction.class);
        verify(transactionRepository).save(txCaptor.capture());
        Transaction saved = txCaptor.getValue();
        assertThat(saved.getUserId()).isEqualTo(userId);
        assertThat(saved.getSourceAccountId()).isEqualTo(sourceAccountId);
        assertThat(saved.getDestinationAccountId()).isEqualTo(destinationAccountId);
        assertThat(saved.getStatus()).isEqualTo(TransactionStatus.PENDING);
        assertThat(saved.getCurrentStep()).isEqualTo(SagaStep.FRAUD_CHECK);

        verify(sagaOrchestrator).requestFraudCheck(saved);
    }

    @Test
    @DisplayName("Sin Idempotency-Key no consulta el repositorio por clave")
    void initiateTransfer_withoutKey_shouldNotLookUpIdempotency() {
        when(transactionRepository.save(any(Transaction.class))).thenAnswer(inv -> inv.getArgument(0));

        paymentService.initiateTransfer(validRequest, userId, null);

        verify(transactionRepository, never()).findByIdempotencyKey(any());
    }

    @Test
    @DisplayName("Debe rechazar la transferencia cuando origen y destino son la misma cuenta")
    void initiateTransfer_whenSameAccount_shouldThrowIllegalArgumentException() {
        TransferRequest selfTransfer = new TransferRequest(
                sourceAccountId.toString(),
                sourceAccountId.toString(),
                new BigDecimal("50.00"),
                "COP",
                "auto-transferencia"
        );

        assertThatThrownBy(() -> paymentService.initiateTransfer(selfTransfer, userId, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("misma cuenta");

        verifyNoInteractions(sagaOrchestrator);
        verify(transactionRepository, never()).save(any());
    }

    @Test
    @DisplayName("Un sourceAccountId que no es UUID debe propagar IllegalArgumentException")
    void initiateTransfer_whenAccountIdIsNotUuid_shouldThrowIllegalArgumentException() {
        TransferRequest malformed = new TransferRequest(
                "no-es-un-uuid",
                destinationAccountId.toString(),
                new BigDecimal("10.00"),
                "USD",
                null
        );

        assertThatThrownBy(() -> paymentService.initiateTransfer(malformed, userId, null))
                .isInstanceOf(IllegalArgumentException.class);

        verifyNoInteractions(sagaOrchestrator);
        verify(transactionRepository, never()).save(any());
    }

    // ---------------------------------------------------------------
    // Idempotencia
    // ---------------------------------------------------------------

    @Test
    @DisplayName("Con una clave nueva guarda la clave en la transacción y crea el pago")
    void initiateTransfer_withNewKey_shouldStoreKeyAndCreatePayment() {
        when(transactionRepository.findByIdempotencyKey("key-nueva")).thenReturn(Optional.empty());
        when(transactionRepository.save(any(Transaction.class))).thenAnswer(inv -> inv.getArgument(0));

        paymentService.initiateTransfer(validRequest, userId, "key-nueva");

        ArgumentCaptor<Transaction> txCaptor = ArgumentCaptor.forClass(Transaction.class);
        verify(transactionRepository).save(txCaptor.capture());
        assertThat(txCaptor.getValue().getIdempotencyKey()).isEqualTo("key-nueva");
        verify(sagaOrchestrator).requestFraudCheck(txCaptor.getValue());
    }

    @Test
    @DisplayName("Misma clave y misma solicitud: devuelve la transacción original sin crear otra")
    void initiateTransfer_withSameKeyAndSameRequest_shouldReturnOriginal() {
        Transaction original = existingTransaction("key-1");
        when(transactionRepository.findByIdempotencyKey("key-1")).thenReturn(Optional.of(original));

        TransferResponse response = paymentService.initiateTransfer(validRequest, userId, "key-1");

        assertThat(response.transactionId()).isEqualTo(original.getTransactionId().toString());
        verify(transactionRepository, never()).save(any());
        verifyNoInteractions(sagaOrchestrator);
    }

    @Test
    @DisplayName("Misma clave con otro monto: DuplicateTransactionException (409)")
    void initiateTransfer_withSameKeyButDifferentAmount_shouldThrowDuplicate() {
        Transaction original = existingTransaction("key-1");
        original.setAmount(new BigDecimal("999.00"));
        when(transactionRepository.findByIdempotencyKey("key-1")).thenReturn(Optional.of(original));

        assertThatThrownBy(() -> paymentService.initiateTransfer(validRequest, userId, "key-1"))
                .isInstanceOf(DuplicateTransactionException.class);

        verify(transactionRepository, never()).save(any());
        verifyNoInteractions(sagaOrchestrator);
    }

    @Test
    @DisplayName("Misma clave con otra cuenta destino: DuplicateTransactionException (409)")
    void initiateTransfer_withSameKeyButDifferentDestination_shouldThrowDuplicate() {
        Transaction original = existingTransaction("key-1");
        original.setDestinationAccountId(UUID.randomUUID());
        when(transactionRepository.findByIdempotencyKey("key-1")).thenReturn(Optional.of(original));

        assertThatThrownBy(() -> paymentService.initiateTransfer(validRequest, userId, "key-1"))
                .isInstanceOf(DuplicateTransactionException.class);
    }

    @Test
    @DisplayName("Clave ya usada por otro usuario: DuplicateTransactionException (409)")
    void initiateTransfer_withKeyUsedByAnotherUser_shouldThrowDuplicate() {
        Transaction original = existingTransaction("key-1");
        original.setUserId(UUID.randomUUID());
        when(transactionRepository.findByIdempotencyKey("key-1")).thenReturn(Optional.of(original));

        assertThatThrownBy(() -> paymentService.initiateTransfer(validRequest, userId, "key-1"))
                .isInstanceOf(DuplicateTransactionException.class);

        verify(transactionRepository, never()).save(any());
        verifyNoInteractions(sagaOrchestrator);
    }

    // ---------------------------------------------------------------
    // getTransactionStatus
    // ---------------------------------------------------------------

    @Test
    @DisplayName("Debe retornar el estado de la transacción cuando existe")
    void getTransactionStatus_whenExists_shouldReturnResponse() {
        UUID transactionId = UUID.randomUUID();
        Transaction transaction = Transaction.builder()
                .transactionId(transactionId)
                .userId(userId)
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destinationAccountId)
                .amount(new BigDecimal("200.00"))
                .currency("USD")
                .status(TransactionStatus.FUNDS_RESERVED)
                .currentStep(SagaStep.RECORD_LEDGER)
                .fraudRiskScore(20.0)
                .build();

        when(transactionRepository.findByTransactionId(transactionId)).thenReturn(Optional.of(transaction));

        TransactionStatusResponse response = paymentService.getTransactionStatus(transactionId);

        assertThat(response.transactionId()).isEqualTo(transactionId.toString());
        assertThat(response.status()).isEqualTo(TransactionStatus.FUNDS_RESERVED);
        assertThat(response.currentStep()).isEqualTo(SagaStep.RECORD_LEDGER);
        assertThat(response.fraudRiskScore()).isEqualTo(20.0);
        assertThat(response.failureReason()).isNull();
    }

    @Test
    @DisplayName("Debe exponer el motivo de fallo cuando fue rechazada por fraude")
    void getTransactionStatus_whenFraudRejected_shouldExposeReason() {
        UUID transactionId = UUID.randomUUID();
        Transaction transaction = Transaction.builder()
                .transactionId(transactionId)
                .userId(userId)
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destinationAccountId)
                .amount(new BigDecimal("200.00"))
                .currency("USD")
                .status(TransactionStatus.FRAUD_REJECTED)
                .currentStep(SagaStep.FRAUD_CHECK)
                .fraudRiskScore(85.0)
                .failureReason("FRAUD_REJECTED:NEW_BENEFICIARY_HIGH_AMOUNT")
                .build();

        when(transactionRepository.findByTransactionId(transactionId)).thenReturn(Optional.of(transaction));

        TransactionStatusResponse response = paymentService.getTransactionStatus(transactionId);

        assertThat(response.status()).isEqualTo(TransactionStatus.FRAUD_REJECTED);
        assertThat(response.fraudRiskScore()).isEqualTo(85.0);
        assertThat(response.failureReason()).isEqualTo("FRAUD_REJECTED:NEW_BENEFICIARY_HIGH_AMOUNT");
    }

    @Test
    @DisplayName("Debe lanzar TransactionNotFoundException cuando la transacción no existe")
    void getTransactionStatus_whenNotFound_shouldThrow() {
        UUID transactionId = UUID.randomUUID();
        when(transactionRepository.findByTransactionId(transactionId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> paymentService.getTransactionStatus(transactionId))
                .isInstanceOf(TransactionNotFoundException.class)
                .hasMessageContaining(transactionId.toString());
    }

    // ---------------------------------------------------------------
    // getUserTransactionHistory
    // ---------------------------------------------------------------

    @Test
    @DisplayName("Debe mapear correctamente el historial de transacciones del usuario")
    void getUserTransactionHistory_shouldReturnMappedList() {
        Transaction t1 = Transaction.builder()
                .transactionId(UUID.randomUUID())
                .userId(userId)
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destinationAccountId)
                .amount(new BigDecimal("10.00"))
                .currency("COP")
                .status(TransactionStatus.COMPLETED)
                .currentStep(SagaStep.CONFIRM_PAYMENT)
                .build();
        Transaction t2 = Transaction.builder()
                .transactionId(UUID.randomUUID())
                .userId(userId)
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destinationAccountId)
                .amount(new BigDecimal("20.00"))
                .currency("COP")
                .status(TransactionStatus.FAILED)
                .currentStep(SagaStep.COMPENSATE)
                .build();

        when(transactionRepository.findByUserIdOrderByCreatedAtDesc(userId)).thenReturn(List.of(t1, t2));

        List<TransactionStatusResponse> history = paymentService.getUserTransactionHistory(userId);

        assertThat(history).hasSize(2);
        assertThat(history.get(0).transactionId()).isEqualTo(t1.getTransactionId().toString());
        assertThat(history.get(1).status()).isEqualTo(TransactionStatus.FAILED);
    }

    @Test
    @DisplayName("Debe retornar lista vacía cuando el usuario no tiene transacciones")
    void getUserTransactionHistory_whenEmpty_shouldReturnEmptyList() {
        when(transactionRepository.findByUserIdOrderByCreatedAtDesc(userId)).thenReturn(List.of());

        assertThat(paymentService.getUserTransactionHistory(userId)).isEmpty();
    }

    // ---------------------------------------------------------------
    // getTransactionsByStatus
    // ---------------------------------------------------------------

    @ParameterizedTest
    @EnumSource(TransactionStatus.class)
    @DisplayName("Debe delegar en el repositorio para cada estado posible")
    void getTransactionsByStatus_shouldDelegateToRepository(TransactionStatus status) {
        when(transactionRepository.findByStatus(status)).thenReturn(List.of());

        List<Transaction> result = paymentService.getTransactionsByStatus(status);

        assertThat(result).isEmpty();
        verify(transactionRepository).findByStatus(status);
    }
}