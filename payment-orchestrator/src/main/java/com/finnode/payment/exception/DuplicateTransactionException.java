package com.finnode.payment.exception;

/**
 * Se lanza cuando una Idempotency-Key ya fue usada con un cuerpo distinto
 * (o por otro usuario). → HTTP 409 Conflict.
 */
public class DuplicateTransactionException extends RuntimeException {

    public DuplicateTransactionException(String idempotencyKey) {
        super("La Idempotency-Key '" + idempotencyKey + "' ya fue utilizada con una solicitud diferente");
    }
}