package com.finnode.payment.dto;

import com.finnode.payment.model.SagaStep;
import com.finnode.payment.model.Transaction;
import com.finnode.payment.model.TransactionStatus;

import java.time.LocalDateTime;

/**
 * DTO de salida para el endpoint GET /payments/{transactionId}.
 *
 * <p>Expone el estado actual del Saga con suficiente detalle para que
 * el frontend muestre el progreso de la transacción y, en caso de fallo,
 * el motivo al usuario.
 *
 * <p>{@code currentStep} es el paso interno de la saga (SagaStep). Con la integración de
 * fraud-engine, mientras se espera la evaluación vale {@code FRAUD_CHECK}.
 *
 * <p>{@code fraudRiskScore} se incluye para transparencia: el usuario
 * puede ver el score que generó su transacción, incluso si fue aprobada.
 * Es {@code null} mientras fraud-engine no ha respondido.
 *
 * <p>{@code failureReason} es {@code null} si la transacción está
 * en curso o fue completada exitosamente. Ej: FRAUD_REJECTED, FRAUD_CHECK_TIMEOUT.
 */
public record TransactionStatusResponse(

        String transactionId,

        TransactionStatus status,

        SagaStep currentStep,

        Double fraudRiskScore,

        String failureReason,

        LocalDateTime createdAt,

        LocalDateTime updatedAt

) {

    /** Construye la respuesta a partir de la entidad (evita duplicar el mapeo en el servicio). */
    public static TransactionStatusResponse from(Transaction transaction) {
        return new TransactionStatusResponse(
                transaction.getTransactionId().toString(),
                transaction.getStatus(),
                transaction.getCurrentStep(),
                transaction.getFraudRiskScore(),
                transaction.getFailureReason(),
                transaction.getCreatedAt(),
                transaction.getUpdatedAt()
        );
    }
}