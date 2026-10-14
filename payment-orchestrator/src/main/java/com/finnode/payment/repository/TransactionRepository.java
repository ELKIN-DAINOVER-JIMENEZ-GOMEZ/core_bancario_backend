package com.finnode.payment.repository;

import com.finnode.payment.model.Transaction;
import com.finnode.payment.model.TransactionStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Repositorio JPA para la entidad {@link Transaction}.
 *
 * Proporciona consultas frecuentes necesarias para el payment-orchestrator:
 *  - buscar por transactionId (clave pública de correlación)
 *  - listar transacciones de un usuario
 *  - filtrar por estado
 *  - historial del usuario ordenado por fecha de creación (desc)
 */
@Repository
public interface TransactionRepository extends JpaRepository<Transaction, UUID> {

    Optional<Transaction> findByTransactionId(UUID transactionId);
    Optional<Transaction> findByIdempotencyKey(String idempotencyKey);

    List<Transaction> findByUserId(UUID userId);

    List<Transaction> findByStatus(TransactionStatus status);

    List<Transaction> findByUserIdOrderByCreatedAtDesc(UUID userId);
}

