package com.finnode.payment.dto;

import com.finnode.payment.model.TransactionStatus;

import java.time.Instant;
import java.util.UUID;

/**
 * Mensaje enviado por WebSocket al usuario cuando su transacción llega a un estado final.
 */
public record PaymentNotification(
        UUID transactionId,
        TransactionStatus status,
        String failureReason,
        String message,
        Instant timestamp
) {
    public static PaymentNotification of(UUID transactionId, TransactionStatus status,
                                         String failureReason, String message) {
        return new PaymentNotification(transactionId, status, failureReason, message, Instant.now());
    }
}