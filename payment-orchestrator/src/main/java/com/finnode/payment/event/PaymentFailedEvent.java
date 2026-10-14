package com.finnode.payment.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Publicado en [payment.failed]: rechazo por fraude, fondos insuficientes o
 * compensación finalizada. failureReason ej: FRAUD_REJECTED, FRAUD_CHECK_TIMEOUT,
 * INSUFFICIENT_FUNDS, LEDGER_TIMEOUT.
 */
public record PaymentFailedEvent(
        UUID transactionId,
        UUID userId,
        UUID sourceAccountId,
        UUID destinationAccountId,
        BigDecimal amount,
        String currency,
        String failureReason,
        Instant timestamp
) {
}