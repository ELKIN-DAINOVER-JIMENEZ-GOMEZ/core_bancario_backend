package com.finnode.payment.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Consumido de [account.funds-released]. Productor: account-service.
 * Confirma que la reserva fue liberada (paso final de la compensación).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FundsReleasedEvent(
        UUID transactionId,
        String accountId,
        BigDecimal amount,
        Instant timestamp
) {
}