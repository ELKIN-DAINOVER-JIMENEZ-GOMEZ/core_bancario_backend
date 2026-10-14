# 🎼 payment-orchestrator — El Director de Orquesta

> Microservicio de FinNode que coordina el flujo completo de una transferencia mediante el **Patrón Saga (Orquestación)**. No mueve dinero directamente: dirige a los demás servicios con eventos de Kafka y decide si continuar o compensar. La evaluación de fraude la realiza el microservicio independiente [`fraud-engine`](./fraud-engine-README.md) (Python + FastAPI), **no** un componente interno.

---

## 📋 Tabla de Contenidos

- [Responsabilidades](#-responsabilidades)
- [Integración con fraud-engine](#-integración-con-fraud-engine)
- [Endpoints](#-endpoints)
- [Estructura de Carpetas](#-estructura-de-carpetas)
- [Descripción de Cada Capa](#-descripción-de-cada-capa)
- [Máquina de Estados de la Saga](#-máquina-de-estados-de-la-saga)
- [Modelo de Base de Datos](#-modelo-de-base-de-datos)
- [Eventos Kafka](#-eventos-kafka)
- [Notificaciones WebSocket](#-notificaciones-websocket)
- [Manejo de Errores](#-manejo-de-errores)
- [Dependencias](#-dependencias)
- [Variables de Entorno](#-variables-de-entorno)
- [Flujos Internos](#-flujos-internos)

---

## ✅ Responsabilidades

| # | Responsabilidad | Descripción |
|---|----------------|-------------|
| 1 | **Recibir la transferencia** | Acepta la solicitud del usuario, la valida, la registra con estado `PENDING` y responde de inmediato (`202 Accepted`) |
| 2 | **Solicitar evaluación de fraude** | Publica `fraud.check-requested` y espera la respuesta `fraud.check-completed` del `fraud-engine` |
| 3 | **Orquestar la saga** | Con fraude aprobado: publica `payment.initiated` (account-service reserva fondos) → `payment.completed` (ledger registra asientos) → `COMPLETED` |
| 4 | **Compensar** | Si el ledger falla, publica `payment.reversed` para revertir lo ya ejecutado, sin intervención manual |
| 5 | **Notificar** | Informa el resultado final al usuario en tiempo real vía WebSocket |
| 6 | **Persistir el estado de la saga** | Guarda el progreso de cada pago (`status` + `currentStep`) para consulta de estado y recuperación ante reinicios |
| 7 | **Idempotencia** | Evita pagos duplicados por reintentos del cliente mediante la cabecera `Idempotency-Key` |

---

## 🛡️ Integración con `fraud-engine`

**El `payment-orchestrator` es el único cliente del `fraud-engine`, y lo hace exclusivamente por Kafka** (sin llamadas HTTP ni Feign).

```
payment-orchestrator                          fraud-engine (Python / FastAPI)
        │                                              │
        │── [fraud.check-requested] ──────────────────▶│  evalúa reglas, guarda evaluación
        │                                              │
        │◀────────────────── [fraud.check-completed] ──│  decision + riskScore + triggeredRules
        │
        ├── APPROVED → continúa la saga: publica payment.initiated (reserva de fondos)
        └── REJECTED → payment = FRAUD_REJECTED (fin; no se tocó ningún saldo)
```

| Aspecto | Comportamiento |
|---------|----------------|
| **Rol** | Productor de `fraud.check-requested`, consumidor de `fraud.check-completed` |
| **Correlación** | Por `transactionId` (UUID), usado también como *key* del mensaje |
| **Momento** | Siempre **antes** de reservar fondos; un rechazo por fraude no requiere compensación. `payment.initiated` (que dispara la reserva) solo se publica tras la aprobación |
| **Decisión desconocida** | Solo `APPROVED` aprueba. Una decisión nula o desconocida se trata como rechazo (*fail-closed*) |
| **Respuesta tardía / duplicada** | Se ignora: el pago ya no está en `PENDING` + `FRAUD_CHECK` |
| **Regla ante rechazo** | `failureReason` = `FRAUD_REJECTED` seguido de los códigos de reglas activadas (ej. `FRAUD_REJECTED:UNUSUAL_HOUR,NEW_BENEFICIARY_HIGH_AMOUNT`), truncado a 500 caracteres (longitud de la columna) |
| **Acoplamiento** | Solo por el contrato de los dos eventos. El orquestador no sabe (ni le importa) si detrás hay reglas o un modelo de ML. El mapper ignora campos desconocidos, por lo que `fraud-engine` puede agregar campos sin romper el orquestador |

---

## 🌐 Endpoints

```
POST /payments/transfer             → Solicitar una transferencia (TransferRequest → TransferResponse, 202)
GET  /payments/{transactionId}      → Consultar el estado (TransactionStatusResponse)
GET  /payments/...                  → Historial del usuario (List<TransactionStatusResponse>)
WS   /ws/payments?token=<jwt>       → WebSocket: notificaciones de estado en tiempo real
```

> Requieren JWT válido, validado por el `api-gateway`. El `userId` se extrae del token.
> `POST` acepta la cabecera opcional `Idempotency-Key`, que `PaymentService.initiateTransfer(request, userId, idempotencyKey)` usa para evitar duplicados.

**Ejemplo de solicitud (`TransferRequest`):**

```json
POST /payments/transfer
Idempotency-Key: 3f2b8c1e-...

{
  "sourceAccountId": "4c5d6e7f-1111-2222-3333-444455556666",
  "destinationAccountId": "7a8b9c0d-aaaa-bbbb-cccc-ddddeeeeffff",
  "amount": 500000.00,
  "currency": "COP",
  "description": "Pago de arriendo"
}
```

**Respuesta (`TransferResponse`):** `202 Accepted`

```json
{
  "transactionId": "9f1e2a3b-7c4d-4e5f-8a9b-0c1d2e3f4a5b",
  "status": "PENDING",
  "amount": 500000.00,
  "currency": "COP",
  "timestamp": "2025-01-15T10:31:00",
  "message": "Transferencia iniciada. Estado: PENDING"
}
```

**Respuesta (`TransactionStatusResponse`):**

```json
{
  "transactionId": "9f1e2a3b-7c4d-4e5f-8a9b-0c1d2e3f4a5b",
  "status": "FRAUD_REJECTED",
  "currentStep": "FRAUD_CHECK",
  "fraudRiskScore": 85.0,
  "failureReason": "FRAUD_REJECTED:NEW_BENEFICIARY_HIGH_AMOUNT",
  "createdAt": "2025-01-15T10:31:00",
  "updatedAt": "2025-01-15T10:31:01"
}
```

> `currentStep` es el paso interno de la saga (`SagaStep`). `fraudRiskScore` es `null` mientras `fraud-engine` no responde y `failureReason` es `null` si el pago está en curso o se completó.

**Idempotencia:**

| Caso | Resultado |
|------|-----------|
| Sin `Idempotency-Key` | Cada solicitud crea un pago nuevo |
| Misma clave y misma solicitud (mismo usuario, cuentas, monto y moneda) | Devuelve la transacción original, sin crear otra |
| Misma clave con otra solicitud, o usada por otro usuario | `409 Conflict` (`DuplicateTransactionException`) |
| Clave de más de 64 caracteres | `400 Bad Request` |

> La idempotencia vive **solo en este servicio**: la cabecera `Idempotency-Key` y la columna `idempotency_key` no se propagan en ningún evento, así que no involucra a `account-service`, `ledger-service`, `fraud-engine` ni `api-gateway` (el gateway solo debe dejar pasar la cabecera).
> Es una función opcional: la saga con `fraud-engine` funciona sin ella, pero es la protección contra pagos duplicados por reintentos del cliente.

---

## 📁 Estructura de Carpetas

```
payment-orchestrator/
├── src/
│   ├── main/
│   │   ├── java/com/finnode/payment/
│   │   │   │
│   │   │   ├── PaymentOrchestratorApplication.java      Punto de entrada del microservicio
│   │   │   │
│   │   │   ├── client/
│   │   │   │   └── AccountClient.java                   @FeignClient hacia account-service (la saga no lo invoca: la reserva se dispara por evento)
│   │   │   │
│   │   │   ├── config/
│   │   │   │   ├── FeignConfig.java                     Timeouts y propagación de JWT hacia account-service
│   │   │   │   ├── KafkaConsumerConfig.java             Consumer de Kafka; ObjectMapper tolerante a campos nuevos y fechas ISO-8601
│   │   │   │   ├── KafkaProducerConfig.java             KafkaTemplate (usa JsonStringSerializer)
│   │   │   │   ├── SecurityConfig.java                  Resource Server JWT; /ws/** se autentica en el handshake
│   │   │   │   ├── WebSocketConfig.java                 Endpoint /ws/payments + validación del JWT en el handshake
│   │   │   │   └── SchedulingConfig.java                Habilita @Scheduled
│   │   │   │
│   │   │   ├── controller/
│   │   │   │   └── PaymentController.java               REST de transferencia, estado e historial
│   │   │   │
│   │   │   ├── dto/
│   │   │   │   ├── TransferRequest.java                 Entrada: cuentas, monto, moneda, descripción
│   │   │   │   ├── TransferResponse.java                Salida: transactionId, status, amount, currency, timestamp, message
│   │   │   │   ├── TransactionStatusResponse.java       Salida de consulta: currentStep, fraudRiskScore, failureReason y el factory from(Transaction)
│   │   │   │   └── PaymentNotification.java             Mensaje enviado por WebSocket
│   │   │   │
│   │   │   ├── event/
│   │   │   │   │   ── Publicados por el orquestador ──
│   │   │   │   ├── FraudCheckRequestedEvent.java        [fraud.check-requested]
│   │   │   │   ├── PaymentInitiatedEvent.java           [payment.initiated] — fraude aprobado, dispara la reserva de fondos
│   │   │   │   ├── PaymentCompletedEvent.java           [payment.completed] — fondos reservados, dispara el registro contable
│   │   │   │   ├── PaymentReversedEvent.java            [payment.reversed] — compensación (ledger + account-service)
│   │   │   │   ├── PaymentFailedEvent.java              [payment.failed] — rechazo por fraude, fallo de reserva o compensación terminada
│   │   │   │   │
│   │   │   │   │   ── Consumidos por el orquestador ──
│   │   │   │   ├── FraudCheckCompletedEvent.java        [fraud.check-completed]
│   │   │   │   ├── FundsReservedEvent.java              [account.funds-reserved]
│   │   │   │   ├── FundsReservationFailedEvent.java     [account.funds-reservation-failed]
│   │   │   │   ├── FundsReleasedEvent.java              [account.funds-released] — confirma la compensación
│   │   │   │   ├── LedgerEntriesRecordedEvent.java      [ledger.entries-recorded]
│   │   │   │   └── LedgerRecordingFailedEvent.java      [ledger.recording-failed]
│   │   │   │
│   │   │   ├── exception/
│   │   │   │   ├── FraudDetectedException.java          Rechazo por fraude → HTTP 422
│   │   │   │   ├── PaymentProcessingException.java      Error genérico del procesamiento de la saga
│   │   │   │   ├── TransactionNotFoundException.java    → HTTP 404
│   │   │   │   ├── DuplicateTransactionException.java   → HTTP 409 (Idempotency-Key repetida con otra solicitud)
│   │   │   │   └── GlobalExceptionHandler.java          @RestControllerAdvice: errores uniformes en JSON
│   │   │   │
│   │   │   ├── kafka/
│   │   │   │   ├── JsonStringSerializer.java            Serialización de eventos a JSON para Kafka
│   │   │   │   ├── PaymentEventConsumer.java            6 listeners: fraude, cuentas (3) y ledger (2)
│   │   │   │   └── PaymentEventPublisher.java           Publica los 5 eventos del orquestador
│   │   │   │
│   │   │   ├── model/
│   │   │   │   ├── Transaction.java                     Entidad JPA con el estado de la saga
│   │   │   │   ├── TransactionStatus.java               ENUM de estado público (ver Máquina de Estados)
│   │   │   │   └── SagaStep.java                        ENUM del paso actual de la saga
│   │   │   │
│   │   │   ├── repository/
│   │   │   │   └── TransactionRepository.java           Consultas por transactionId, usuario, estado e idempotencyKey
│   │   │   │
│   │   │   ├── service/
│   │   │   │   ├── PaymentService.java                  Crea la transacción, idempotencia, consulta de estado e historial
│   │   │   │   └── SagaOrchestrator.java                Máquina de estados: avanza/compensa según cada evento
│   │   │   │
│   │   │   └── websocket/
│   │   │       └── PaymentNotificationHandler.java      Mantiene sesiones por userId y envía el estado final
│   │   │
│   │   └── resources/
│   │       └── application.yml                          Configuración del servicio
│   │
│   └── test/
│       └── java/com/finnode/payment/
│           ├── PaymentOrchestratorApplicationTests.java   Arranque del contexto de Spring
│           └── service/
│               ├── PaymentServiceTest.java                Unit tests: creación, idempotencia, validaciones, consultas
│               ├── SagaOrchestratorTest.java              Unit tests: reserva de fondos, ledger y compensación
│               └── SagaOrchestratorFraudFlowTest.java     Unit tests: respuesta de fraude (aprobado/rechazado) y fondos liberados
│
├── pom.xml
└── Dockerfile
```

---

## 📐 Descripción de Cada Capa

### `service/`
| Clase | Qué hace |
|-------|----------|
| `PaymentService` | Valida la solicitud (origen ≠ destino), aplica idempotencia, persiste la `Transaction` (`PENDING` / `FRAUD_CHECK`) y pide la evaluación de fraude al orquestador. También atiende consulta de estado, historial por usuario y consulta por estado (diagnóstico) |
| `SagaOrchestrator` | Máquina de estados. Recibe cada evento (fraude, cuentas, ledger), valida que la transición sea legal, ejecuta el siguiente paso o dispara la compensación. Es el único que conoce el flujo completo |

> 🔒 **Garantía de orden:** los eventos de Kafka y las notificaciones WebSocket se emiten **después del commit** de la transacción de base de datos. Así, una respuesta muy rápida de otro servicio (por ejemplo `fraud-engine`) nunca llega antes de que el nuevo estado esté persistido. Si falla la publicación, el estado ya guardado no se revierte.

### `client/`
`AccountClient` (OpenFeign) define los comandos síncronos hacia `account-service` (`reserve`, `confirm-debit`, `credit`). La saga no lo invoca: la reserva de fondos se dispara por el evento `payment.initiated`. **No existe ningún cliente hacia el motor de fraude**; esa comunicación es solo por Kafka.

### `kafka/`
| Clase | Qué hace |
|-------|----------|
| `JsonStringSerializer` | Serializa los eventos a JSON para el `KafkaTemplate` |
| `PaymentEventConsumer` | Escucha `fraud.check-completed`, `account.funds-reserved`, `account.funds-reservation-failed`, `account.funds-released`, `ledger.entries-recorded` y `ledger.recording-failed`; delega todo a `SagaOrchestrator`. Captura y registra las excepciones de cada listener |
| `PaymentEventPublisher` | Publica `fraud.check-requested`, `payment.initiated`, `payment.completed`, `payment.reversed` y `payment.failed`. La *key* es siempre el `transactionId` |

### `config/`
`KafkaConsumerConfig` recibe los mensajes como `String` y `StringJsonMessageConverter` los convierte al tipo del parámetro del `@KafkaListener`, sin depender de cabeceras de tipo. Por eso puede consumir eventos de servicios que no son Java (como `fraud-engine`). El `ObjectMapper` ignora campos desconocidos y escribe fechas en ISO-8601.

### `model/`
| Clase | Qué hace |
|-------|----------|
| `Transaction` | Entidad que guarda el estado de la saga y los datos de la transferencia (ver [Modelo de Base de Datos](#-modelo-de-base-de-datos)) |
| `TransactionStatus` | Estado **público** de la transacción |
| `SagaStep` | Paso **interno** actual de la saga; permite saber exactamente dónde está cada pago |

### `websocket/`
`PaymentNotificationHandler` mantiene las sesiones por `userId` (varias por usuario) y envía un `PaymentNotification` cuando la transacción llega a un estado final. Si el usuario no está conectado no pasa nada: puede consultar el `GET` de estado.

---

## 🔀 Máquina de Estados de la Saga

El progreso se modela con dos enums del proyecto: **`TransactionStatus`** (estado público) y **`SagaStep`** (paso interno). Se persisten juntos en cada transición.

```
PENDING / FRAUD_CHECK  ── REJECTED / decisión desconocida ──▶ FRAUD_REJECTED       (fin, sin compensación)
   │
   │ APPROVED → publica payment.initiated
   ▼
PENDING / RESERVE_FUNDS ── funds-reservation-failed ─────────▶ FAILED              (fin, nada que revertir)
   │
   │ funds-reserved → publica payment.completed
   ▼
FUNDS_RESERVED / RECORD_LEDGER ── ledger.recording-failed ───▶ REVERSED / COMPENSATE
   │                                (publica payment.reversed)        │
   │ ledger.entries-recorded                                          │ account.funds-released
   ▼                                                                  ▼
COMPLETED / CONFIRM_PAYMENT                                  payment.failed + notificación
```

| `TransactionStatus` | `SagaStep` | Significado | ¿Final? |
|---------------------|-----------|-------------|---------|
| `PENDING` | `FRAUD_CHECK` | Solicitud registrada; esperando `fraud.check-completed` | No |
| `FRAUD_REJECTED` | `FRAUD_CHECK` | `fraud-engine` rechazó (o decisión inválida) | Sí |
| `PENDING` | `RESERVE_FUNDS` | Fraude aprobado; esperando `account.funds-reserved` / `account.funds-reservation-failed` | No |
| `FAILED` | `COMPENSATE` | Falló la reserva de fondos; no había nada que revertir | Sí |
| `FUNDS_RESERVED` | `RECORD_LEDGER` | `payment.completed` publicado; esperando asientos del ledger | No |
| `COMPLETED` | `CONFIRM_PAYMENT` | Asientos registrados | Sí |
| `REVERSED` | `COMPENSATE` | Compensación iniciada (`payment.reversed` publicado). Cuando llega `account.funds-released` se confirma y se notifica | Sí |

> `REVERSED` se asigna al **iniciar** la compensación; la confirmación `account.funds-released` no cambia el estado: publica `payment.failed` y notifica al usuario.
> **Guardas de transición:** cada handler ignora el evento si la transacción no está en el estado/paso esperado (eventos duplicados, tardíos o fuera de orden). Por ejemplo, `account.funds-reserved` solo se acepta en `PENDING` + `RESERVE_FUNDS`.
> Al hacer el chequeo de fraude **antes** de reservar fondos, el camino de rechazo es barato: no se bloquea ni se revierte dinero.
> `payment.completed` significa "fondos reservados y pago listo para contabilizar" (es el trigger que consume el `ledger-service`). La transacción solo pasa a `COMPLETED` cuando el ledger confirma.

---

## 🗄️ Modelo de Base de Datos

**Tabla: `transactions`** (entidad `Transaction`)

```
┌──────────────────────────┬──────────────────┬──────────────────────────────────────────────────┐
│ Columna / campo          │ Tipo             │ Descripción                                       │
├──────────────────────────┼──────────────────┼──────────────────────────────────────────────────┤
│ id                       │ UUID (PK)        │ Identificador interno                             │
│ transaction_id           │ UUID UQ          │ ID público de la transacción (key en Kafka)       │
│ idempotency_key          │ VARCHAR(64) UQ   │ Evita pagos duplicados (nullable, inmutable)      │
│ user_id                  │ UUID             │ Usuario que solicita el pago                      │
│ source_account_id        │ UUID             │ Cuenta origen                                     │
│ destination_account_id   │ UUID             │ Cuenta destino                                    │
│ amount                   │ DECIMAL(19,4)    │ Monto                                             │
│ currency                 │ VARCHAR(3)       │ Moneda                                            │
│ description              │ VARCHAR(255)     │ Concepto                                          │
│ status                   │ VARCHAR(20)      │ ENUM TransactionStatus                            │
│ current_step             │ VARCHAR(30)      │ ENUM SagaStep (campo `currentStep`)               │
│ failure_reason           │ VARCHAR(500)     │ Ej: FRAUD_REJECTED:RULE, INSUFFICIENT_FUNDS, motivo del ledger │
│ fraud_risk_score         │ DOUBLE           │ Score de fraud-engine (escala 0–100; nullable)    │
│ created_at               │ TIMESTAMP        │ Creación                                          │
│ updated_at               │ TIMESTAMP        │ Último cambio de estado                           │
└──────────────────────────┴──────────────────┴──────────────────────────────────────────────────┘
```

> La tabla refleja la entidad `Transaction`. `idempotency_key` tiene un índice único (`idx_transactions_idempotency_key`).
> Las reglas de fraude activadas (`triggeredRules`) **no se guardan en una columna**: sus códigos quedan resumidos en `failure_reason`.
> ⚠️ `payment-orchestrator` tiene su **propia instancia de PostgreSQL** aislada (`payment-db`).

---

## 📨 Eventos Kafka

> Todos los `transactionId` son **UUID**. La *key* de cada mensaje es el `transactionId`.

### Publicados

| Topic | Evento | Cuándo | Consumidor |
|-------|--------|--------|------------|
| `fraud.check-requested` | `FraudCheckRequestedEvent` | Al registrar el pago (`PENDING` / `FRAUD_CHECK`), después del commit | **fraud-engine** |
| `payment.initiated` | `PaymentInitiatedEvent` | Al recibir `APPROVED` de fraude | **account-service** (reserva de fondos) |
| `payment.completed` | `PaymentCompletedEvent` | Al recibir `account.funds-reserved` | **ledger-service** (crea asientos DEBIT/CREDIT) |
| `payment.reversed` | `PaymentReversedEvent` | Al compensar | **ledger-service** (asientos de reversión) y **account-service** (libera reserva) |
| `payment.failed` | `PaymentFailedEvent` | Rechazo por fraude, fallo de reserva o compensación confirmada | Interesados (auditoría, notificaciones) |

### Consumidos

| Topic | Evento | Productor | Acción del orquestador |
|-------|--------|-----------|------------------------|
| `fraud.check-completed` | `FraudCheckCompletedEvent` | **fraud-engine** | `APPROVED` → publica `payment.initiated` · otro → `FRAUD_REJECTED` + `payment.failed` + WebSocket |
| `account.funds-reserved` | `FundsReservedEvent` | account-service | `FUNDS_RESERVED` y publica `payment.completed` |
| `account.funds-reservation-failed` | `FundsReservationFailedEvent` | account-service | `FAILED` (motivo del evento) + `payment.failed` + WebSocket |
| `account.funds-released` | `FundsReleasedEvent` | account-service | Confirma la compensación: `payment.failed` + WebSocket |
| `ledger.entries-recorded` | `LedgerEntriesRecordedEvent` | ledger-service | `COMPLETED` + WebSocket |
| `ledger.recording-failed` | `LedgerRecordingFailedEvent` | ledger-service | `REVERSED` y publica `payment.reversed` |

### Contratos (payloads)

**`fraud.check-requested`**

```json
{
  "transactionId": "9f1e2a3b-7c4d-4e5f-8a9b-0c1d2e3f4a5b",
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "sourceAccountId": "4c5d6e7f-1111-2222-3333-444455556666",
  "destinationAccountId": "7a8b9c0d-aaaa-bbbb-cccc-ddddeeeeffff",
  "amount": 500000.00,
  "currency": "COP",
  "timestamp": "2025-01-15T10:31:00Z"
}
```

**`fraud.check-completed`**

```json
{
  "transactionId": "9f1e2a3b-7c4d-4e5f-8a9b-0c1d2e3f4a5b",
  "decision": "APPROVED",
  "riskScore": 30,
  "triggeredRules": [
    { "code": "UNUSUAL_HOUR", "weight": 10, "reason": "Transacción a las 02:14 (America/Bogota)" },
    { "code": "NEW_BENEFICIARY_HIGH_AMOUNT", "weight": 20, "reason": "Primer envío a este destino con monto alto" }
  ],
  "evaluatedAt": "2025-01-15T10:31:00.120Z"
}
```

`decision` ∈ `APPROVED` | `REJECTED`.

**`payment.initiated`**

```json
{
  "transactionId": "9f1e2a3b-7c4d-4e5f-8a9b-0c1d2e3f4a5b",
  "sourceAccountId": "4c5d6e7f-1111-2222-3333-444455556666",
  "amount": 500000.00,
  "currency": "COP",
  "timestamp": "2025-01-15T10:31:01Z"
}
```

**`payment.completed`** (contrato definido por `ledger-service`)

```json
{
  "transactionId": "9f1e2a3b-7c4d-4e5f-8a9b-0c1d2e3f4a5b",
  "sourceAccountId": "4c5d6e7f-1111-2222-3333-444455556666",
  "destinationAccountId": "7a8b9c0d-aaaa-bbbb-cccc-ddddeeeeffff",
  "amount": 500000.00,
  "currency": "COP",
  "timestamp": "2025-01-15T10:31:10Z"
}
```

**`payment.reversed`** (contrato definido por `ledger-service`)

```json
{
  "transactionId": "9f1e2a3b-7c4d-4e5f-8a9b-0c1d2e3f4a5b",
  "sourceAccountId": "4c5d6e7f-1111-2222-3333-444455556666",
  "destinationAccountId": "7a8b9c0d-aaaa-bbbb-cccc-ddddeeeeffff",
  "amount": 500000.00,
  "currency": "COP",
  "reversalReason": "<reason del LedgerRecordingFailedEvent>",
  "timestamp": "2025-01-15T10:31:45Z"
}
```

`reversalReason` es el `reason` que envía el ledger en `ledger.recording-failed`.

**`payment.failed`**

```json
{
  "transactionId": "9f1e2a3b-7c4d-4e5f-8a9b-0c1d2e3f4a5b",
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "sourceAccountId": "4c5d6e7f-1111-2222-3333-444455556666",
  "destinationAccountId": "7a8b9c0d-aaaa-bbbb-cccc-ddddeeeeffff",
  "amount": 500000.00,
  "currency": "COP",
  "failureReason": "FRAUD_REJECTED:NEW_BENEFICIARY_HIGH_AMOUNT",
  "timestamp": "2025-01-15T10:31:01Z"
}
```

**`account.funds-released`**

```json
{
  "transactionId": "9f1e2a3b-7c4d-4e5f-8a9b-0c1d2e3f4a5b",
  "accountId": "4c5d6e7f-1111-2222-3333-444455556666",
  "amount": 500000.00,
  "timestamp": "2025-01-15T10:31:46Z"
}
```

---

## 🔔 Notificaciones WebSocket

**Conexión:** `ws://<host>/ws/payments?token=<access_token>`

El navegador no puede enviar cabeceras en el handshake de WebSocket, por eso el JWT viaja como *query param* `token`. El servicio lo valida con el mismo `JwtDecoder` del Resource Server y extrae el `userId` del claim `userId` (o del `subject` si no existe). Sin token, o con token inválido, el handshake responde `401`. Por eso `/ws/**` no requiere autenticación HTTP previa: la autenticación se hace en el handshake.

**Cuándo se notifica:**

| Evento | `status` enviado |
|--------|------------------|
| Fraude rechazado | `FRAUD_REJECTED` |
| Falla la reserva de fondos | `FAILED` |
| Ledger confirma asientos | `COMPLETED` |
| `account.funds-released` (compensación confirmada) | `REVERSED` |

**Mensaje (`PaymentNotification`):**

```json
{
  "transactionId": "9f1e2a3b-7c4d-4e5f-8a9b-0c1d2e3f4a5b",
  "status": "FRAUD_REJECTED",
  "failureReason": "FRAUD_REJECTED:NEW_BENEFICIARY_HIGH_AMOUNT",
  "message": "Tu transferencia fue rechazada por seguridad",
  "timestamp": "2025-01-15T10:31:01Z"
}
```

> Los orígenes permitidos se configuran con `websocket.allowed-origins` (por defecto `*`).

---

## 🚨 Manejo de Errores

`GlobalExceptionHandler` devuelve un JSON uniforme:

```json
{
  "timestamp": "2025-01-15T10:31:00Z",
  "status": 409,
  "error": "Conflict",
  "message": "La Idempotency-Key '3f2b8c1e-...' ya fue utilizada con una solicitud diferente",
  "path": "/payments/transfer",
  "details": null
}
```

| Excepción | HTTP |
|-----------|------|
| `DuplicateTransactionException` | 409 Conflict |
| `TransactionNotFoundException` | 404 Not Found |
| `FraudDetectedException` | 422 Unprocessable Entity |
| `MethodArgumentNotValidException` (`@Valid`) | 400 (con `details` por campo) |
| `MissingRequestHeaderException` | 400 |
| `IllegalArgumentException` (ej. origen = destino) | 400 |
| `AccessDeniedException` | 403 |
| `PaymentProcessingException` | 500 |
| Cualquier otra | 500 (mensaje genérico, el detalle solo va al log) |

---

## 📦 Dependencias

**`pom.xml` — Spring Initializr:**

| Dependencia | Uso |
|-------------|-----|
| `spring-boot-starter-web` | Endpoints REST |
| `spring-boot-starter-websocket` | Notificaciones en tiempo real al usuario |
| `spring-boot-starter-data-jpa` | Persistencia del estado de la saga |
| `postgresql` | Driver JDBC de PostgreSQL |
| `spring-boot-starter-security` | Seguridad base |
| `spring-boot-starter-oauth2-resource-server` | Validación de JWT (REST y handshake WebSocket) |
| `spring-kafka` | Productor y consumidor de eventos |
| `spring-cloud-starter-openfeign` | Cliente HTTP declarativo hacia `account-service` (`AccountClient`) |
| `spring-boot-starter-validation` | Validaciones `@Valid`, `@Positive` |
| `lombok` | Reducir boilerplate |

---

## ⚙️ Variables de Entorno

El consumer de Kafka se configura por código (`KafkaConsumerConfig`) leyendo estas variables; el resto va en `application.yml`:

```yaml
server:
  port: 8083

spring:
  datasource:
    url: jdbc:postgresql://payment-db:5432/payment_db
    username: ${DB_USER}
    password: ${DB_PASSWORD}

  jpa:
    hibernate:
      ddl-auto: validate
    show-sql: false

  security:
    oauth2:
      resourceserver:
        jwt:
          issuer-uri: ${JWT_ISSUER_URI}

account-service:
  url: ${ACCOUNT_SERVICE_URL}          # Feign

websocket:
  allowed-origins: ${WEBSOCKET_ALLOWED_ORIGINS:*}
```

| Variable | Descripción | Ejemplo |
|----------|-------------|---------|
| `DB_USER` / `DB_PASSWORD` | Credenciales de PostgreSQL | `payment_user` / `s3cr3t` |
| `KAFKA_BOOTSTRAP_SERVERS` | Host y puerto de Kafka (lo lee `KafkaConsumerConfig`; por defecto `localhost:9092`) | `kafka:9092` |
| `SPRING_KAFKA_CONSUMER_GROUP_ID` | Consumer group (por defecto `payment-orchestrator-group`) | `payment-orchestrator-group` |
| `JWT_ISSUER_URI` | Emisor JWT para validación | `http://auth-service:8081` |
| `ACCOUNT_SERVICE_URL` | URL interna del account-service | `http://account-service:8082` |
| `WEBSOCKET_ALLOWED_ORIGINS` | Orígenes permitidos para el WebSocket | `https://app.finnode.com` |

---

## 🔄 Flujos Internos

### Inicio de una transferencia

```
POST /payments/transfer  (Idempotency-Key opcional)
        │
        ▼
PaymentController
        │  @Valid TransferRequest
        ▼
PaymentService.initiateTransfer(request, userId, idempotencyKey)
        ├── Convierte los IDs a UUID
        ├── origen == destino → IllegalArgumentException (400)
        ├── Idempotency-Key existente:
        │       ├── misma solicitud → devuelve la transacción original
        │       └── otra solicitud / otro usuario → DuplicateTransactionException (409)
        ├── Persiste Transaction (status=PENDING, currentStep=FRAUD_CHECK)
        ├── SagaOrchestrator.requestFraudCheck()
        │       └── (tras el commit) publica FraudCheckRequestedEvent → Kafka [fraud.check-requested]
        └── Responde 202 Accepted (TransferResponse: transactionId, status=PENDING, ...)
```

### Respuesta del motor de fraude

```
Kafka [fraud.check-completed]
        │
        ▼
PaymentEventConsumer.onFraudCheckCompleted()
        │
        ▼
SagaOrchestrator.onFraudCheckCompleted()
        ├── Busca Transaction por transactionId (UUID)
        ├── Si no está en PENDING + FRAUD_CHECK → ignora (duplicado o respuesta tardía)
        ├── APPROVED → guarda fraudRiskScore
        │       └── startSaga(): currentStep=RESERVE_FUNDS
        │               └── (tras el commit) publica PaymentInitiatedEvent → Kafka [payment.initiated]
        └── REJECTED / desconocido → rejectAsFraud(): status=FRAUD_REJECTED
                ├── Guarda fraudRiskScore y failureReason (FRAUD_REJECTED:códigos)
                └── (tras el commit) publica PaymentFailedEvent → Kafka [payment.failed]
                                     + notifica al usuario por WebSocket
```

### Reserva de fondos y registro contable

```
Kafka [account.funds-reserved]
        └── SagaOrchestrator.onFundsReserved()   (solo en PENDING + RESERVE_FUNDS)
                ├── status=FUNDS_RESERVED, currentStep=RECORD_LEDGER
                └── (tras el commit) publica PaymentCompletedEvent → Kafka [payment.completed]  (ledger crea DEBIT/CREDIT)

Kafka [account.funds-reservation-failed]
        └── SagaOrchestrator.onFundsReservationFailed()   (solo en PENDING + RESERVE_FUNDS)
                ├── status=FAILED, failureReason=motivo del evento
                └── (tras el commit) PaymentFailedEvent [payment.failed] + notificación WebSocket

Kafka [ledger.entries-recorded]
        └── SagaOrchestrator.onLedgerRecorded()   (solo en FUNDS_RESERVED)
                ├── status=COMPLETED, currentStep=CONFIRM_PAYMENT
                └── (tras el commit) notificación WebSocket
```

### Compensación (Saga)

```
Kafka [ledger.recording-failed]
        │
        ▼
PaymentEventConsumer.onLedgerRecordingFailed()
        │
        ▼
SagaOrchestrator.compensateTransaction(transactionId, reason)   (solo en FUNDS_RESERVED)
        ├── status=REVERSED, currentStep=COMPENSATE, failureReason=reason
        └── (tras el commit) publica PaymentReversedEvent → Kafka [payment.reversed]
                ├── ledger-service:  crea asientos REVERSAL_DEBIT / REVERSAL_CREDIT (si había asientos)
                └── account-service: libera la reserva → publica account.funds-released

Kafka [account.funds-released]
        └── SagaOrchestrator.onFundsReleased()   (solo en REVERSED)
                └── (tras el commit) PaymentFailedEvent [payment.failed] + notificación WebSocket
```

### Conexión WebSocket

```
WS /ws/payments?token=<jwt>
        │
        ▼
JwtHandshakeInterceptor
        ├── Sin token / token inválido → 401, no se abre la conexión
        └── Token válido → guarda el userId en los atributos de la sesión
                │
                ▼
PaymentNotificationHandler.afterConnectionEstablished()
        └── Registra la sesión (envuelta para envío concurrente seguro) bajo su userId

SagaOrchestrator (estado final) → PaymentNotificationHandler.notifyUser(userId, PaymentNotification)
        └── Envía el JSON a todas las sesiones abiertas de ese usuario
```

---