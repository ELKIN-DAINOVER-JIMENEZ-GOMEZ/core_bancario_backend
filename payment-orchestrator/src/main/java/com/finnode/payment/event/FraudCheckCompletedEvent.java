package com.finnode.payment.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Consumido de [fraud.check-completed] (key = transactionId). Productor: fraud-engine.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FraudCheckCompletedEvent(
        UUID transactionId,
        Decision decision,
        Integer riskScore,
        List<TriggeredRule> triggeredRules,
        Instant evaluatedAt
) {

    public enum Decision {
        APPROVED,
        REJECTED
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TriggeredRule(String code, Integer weight, String reason) {
    }

    /** Solo APPROVED aprueba. Una decisión nula o desconocida se trata como rechazo (fail-closed). */
    public boolean isApproved() {
        return decision == Decision.APPROVED;
    }
}