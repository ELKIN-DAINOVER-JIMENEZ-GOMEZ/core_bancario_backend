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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Servicio principal de pagos.
 *
 * Orquesta el punto de entrada: recibe el TransferRequest, crea la entidad
 * Transaction en base de datos y arranca el Saga solicitando la evaluación de
 * fraude a fraud-engine (vía Kafka, de forma asíncrona).
 *
 * También atiende consultas de estado e historial de transacciones.
 *
 * Flujo:
 * 1. Recibe solicitud de transferencia (POST /payments/transfer, cabecera Idempotency-Key opcional)
 * 2. Si la Idempotency-Key ya existe con la misma solicitud → devuelve la transacción original
 * 3. Valida que sourceAccountId != destinationAccountId
 * 4. Persiste Transaction (status: PENDING, step: FRAUD_CHECK)
 * 5. SagaOrchestrator.requestFraudCheck() → publica fraud.check-requested (tras el commit)
 * 6. Retorna TransferResponse con transactionId y status PENDING (HTTP 202 Accepted)
 *
 * El resultado del fraude llega después por fraud.check-completed:
 *  - APPROVED → el Saga continúa con la reserva de fondos
 *  - REJECTED → FRAUD_REJECTED; el usuario se entera por WebSocket o por GET de estado
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    /** Longitud de la columna transactions.idempotency_key. */
    private static final int MAX_IDEMPOTENCY_KEY_LENGTH = 64;

    private final TransactionRepository transactionRepository;
    private final SagaOrchestrator sagaOrchestrator;

    /**
     * Inicia una transferencia sin Idempotency-Key.
     * Mantiene la firma anterior para no romper el controller.
     */
    @Transactional
    public TransferResponse initiateTransfer(TransferRequest request, UUID userId) {
        return initiateTransfer(request, userId, null);
    }

    /**
     * Inicia una transferencia entre cuentas.
     *
     * Entrada: TransferRequest con sourceAccountId, destinationAccountId, amount, currency, description
     * Salida: TransferResponse con transactionId, status PENDING (HTTP 202 Accepted)
     *
     * @param request los datos de la transferencia
     * @param userId el usuario autenticado (extraído del JWT)
     * @param idempotencyKey valor de la cabecera Idempotency-Key (puede ser null)
     * @return TransferResponse con datos de la transacción iniciada
     * @throws IllegalArgumentException si sourceAccountId == destinationAccountId
     * @throws DuplicateTransactionException si la Idempotency-Key ya se usó con otra solicitud (HTTP 409)
     */
    @Transactional
    public TransferResponse initiateTransfer(TransferRequest request, UUID userId, String idempotencyKey) {
        log.info("Initiating transfer for user: {}", userId);

        // Convertir strings a UUIDs
        UUID sourceAccountId = UUID.fromString(request.sourceAccountId());
        UUID destinationAccountId = UUID.fromString(request.destinationAccountId());

        // Validar que no sea una transferencia a la misma cuenta
        if (sourceAccountId.equals(destinationAccountId)) {
            log.warn("User {} attempted self-transfer", userId);
            throw new IllegalArgumentException("No puedes transferir a la misma cuenta");
        }

        // Idempotencia: un reintento del cliente no debe crear un segundo pago
        String key = normalize(idempotencyKey);
        if (key != null) {
            Optional<Transaction> existing = transactionRepository.findByIdempotencyKey(key);
            if (existing.isPresent()) {
                return replay(existing.get(), request, userId, sourceAccountId, destinationAccountId, key);
            }
        }

        // TODO: En producción, consultar account-service via Feign para validar que las
        // cuentas existen y están activas. Por ahora, asumimos que son válidas.

        // Crear entidad de transacción: queda esperando la evaluación de fraude
        Transaction transaction = Transaction.builder()
                .transactionId(UUID.randomUUID())
                .idempotencyKey(key)
                .userId(userId)
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destinationAccountId)
                .amount(request.amount())
                .currency(request.currency())
                .description(request.description())
                .status(TransactionStatus.PENDING)
                .currentStep(SagaStep.FRAUD_CHECK)
                .build();

        transaction = transactionRepository.save(transaction);
        log.debug("Created transaction entity: {}", transaction.getTransactionId());

        // Solicitar evaluación de fraude a fraud-engine (se publica tras el commit)
        sagaOrchestrator.requestFraudCheck(transaction);

        // Retornar respuesta inmediata (202 Accepted)
        return new TransferResponse(
                transaction.getTransactionId().toString(),
                TransactionStatus.PENDING,
                request.amount(),
                request.currency(),
                LocalDateTime.now(),
                "Transferencia iniciada. Estado: PENDING"
        );
    }

    /**
     * Consulta el estado de una transacción.
     *
     * Entrada: transactionId (UUID)
     * Salida: TransactionStatusResponse con estado actual, step, scores, etc.
     *
     * @param transactionId el identificador de la transacción
     * @return TransactionStatusResponse con detalles de la transacción
     * @throws TransactionNotFoundException si no existe la transacción
     */
    @Transactional(readOnly = true)
    public TransactionStatusResponse getTransactionStatus(UUID transactionId) {
        log.debug("Querying status for transaction: {}", transactionId);

        Transaction transaction = transactionRepository
                .findByTransactionId(transactionId)
                .orElseThrow(() -> new TransactionNotFoundException(transactionId.toString()));

        return TransactionStatusResponse.from(transaction);
    }

    /**
     * Obtiene el historial de transacciones del usuario autenticado.
     *
     * Retorna todas las transacciones del usuario, ordenadas por fecha
     * de creación descendente (más reciente primero).
     *
     * @param userId el identificador del usuario
     * @return lista de TransactionStatusResponse
     */
    @Transactional(readOnly = true)
    public List<TransactionStatusResponse> getUserTransactionHistory(UUID userId) {
        log.debug("Querying transaction history for user: {}", userId);

        return transactionRepository
                .findByUserIdOrderByCreatedAtDesc(userId)
                .stream()
                .map(TransactionStatusResponse::from)
                .collect(Collectors.toList());
    }

    /**
     * Obtiene todas las transacciones en un estado específico.
     *
     * Útil para diagnóstico y monitoring.
     *
     * @param status el TransactionStatus a filtrar
     * @return lista de transacciones en ese estado
     */
    @Transactional(readOnly = true)
    public List<Transaction> getTransactionsByStatus(TransactionStatus status) {
        log.debug("Querying transactions with status: {}", status);
        return transactionRepository.findByStatus(status);
    }

    // ───────────────────────── utilidades ─────────────────────────

    /**
     * Misma Idempotency-Key + misma solicitud → devuelve la transacción original.
     * Misma clave con otro cuerpo (o de otro usuario) → 409.
     */
    private TransferResponse replay(Transaction existing, TransferRequest request, UUID userId,
                                    UUID sourceAccountId, UUID destinationAccountId, String key) {
        boolean sameRequest = existing.getUserId().equals(userId)
                && existing.getSourceAccountId().equals(sourceAccountId)
                && existing.getDestinationAccountId().equals(destinationAccountId)
                && existing.getAmount().compareTo(request.amount()) == 0
                && existing.getCurrency().equals(request.currency());

        if (!sameRequest) {
            log.warn("Idempotency-Key {} reused with a different request by user {}", key, userId);
            throw new DuplicateTransactionException(key);
        }

        log.info("Idempotency-Key repeated, returning existing transaction: {}", existing.getTransactionId());
        return new TransferResponse(
                existing.getTransactionId().toString(),
                existing.getStatus(),
                existing.getAmount(),
                existing.getCurrency(),
                existing.getCreatedAt(),
                "Transferencia ya registrada con esta Idempotency-Key. Estado: " + existing.getStatus()
        );
    }

    private String normalize(String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return null;
        }
        String key = idempotencyKey.trim();
        if (key.length() > MAX_IDEMPOTENCY_KEY_LENGTH) {
            throw new IllegalArgumentException(
                    "La Idempotency-Key no puede superar " + MAX_IDEMPOTENCY_KEY_LENGTH + " caracteres");
        }
        return key;
    }
}