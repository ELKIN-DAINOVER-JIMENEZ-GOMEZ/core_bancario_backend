package com.finnode.payment.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Publicado en [fraud.check-requested] (key = transactionId). Consumidor: fraud-engine.
 */
public record FraudCheckRequestedEvent(
        UUID transactionId,
        UUID userId,
        UUID sourceAccountId,
        UUID destinationAccountId,
        BigDecimal amount,
        String currency,
        Instant timestamp
) {
}