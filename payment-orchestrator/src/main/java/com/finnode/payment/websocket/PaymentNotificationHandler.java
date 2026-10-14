package com.finnode.payment.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.finnode.payment.dto.PaymentNotification;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mantiene las sesiones WebSocket por userId y envía un PaymentNotification
 * cuando una transacción llega a un estado final.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentNotificationHandler extends TextWebSocketHandler {

    public static final String USER_ID_ATTRIBUTE = "userId";

    private static final int SEND_TIME_LIMIT_MS = 5_000;
    private static final int BUFFER_SIZE_LIMIT_BYTES = 64 * 1024;

    private final ObjectMapper objectMapper;
    private final Map<String, Set<WebSocketSession>> sessionsByUser = new ConcurrentHashMap<>();

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        Object userId = session.getAttributes().get(USER_ID_ATTRIBUTE);
        if (userId == null) {
            session.close(CloseStatus.POLICY_VIOLATION);
            return;
        }
        // WebSocketSession no es thread-safe para escritura: se envuelve.
        WebSocketSession safeSession =
                new ConcurrentWebSocketSessionDecorator(session, SEND_TIME_LIMIT_MS, BUFFER_SIZE_LIMIT_BYTES);
        sessionsByUser.computeIfAbsent(userId.toString(), k -> ConcurrentHashMap.newKeySet()).add(safeSession);
        log.debug("WebSocket conectado: user={} session={}", userId, session.getId());
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        removeSession(session);
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
        log.warn("Error de transporte WebSocket session={}: {}", session.getId(), exception.getMessage());
        removeSession(session);
        if (session.isOpen()) {
            session.close(CloseStatus.SERVER_ERROR);
        }
    }

    /**
     * Envía la notificación a todas las sesiones abiertas del usuario.
     * Si el usuario no está conectado, no pasa nada: puede consultar GET /payments/{id}.
     */
    public void notifyUser(UUID userId, PaymentNotification notification) {
        Set<WebSocketSession> sessions = sessionsByUser.get(userId.toString());
        if (sessions == null || sessions.isEmpty()) {
            return;
        }
        final TextMessage message;
        try {
            message = new TextMessage(objectMapper.writeValueAsString(notification));
        } catch (IOException e) {
            log.error("No se pudo serializar la notificación de {}", notification.transactionId(), e);
            return;
        }
        for (WebSocketSession session : sessions) {
            try {
                if (session.isOpen()) {
                    session.sendMessage(message);
                } else {
                    removeSession(session);
                }
            } catch (Exception e) {
                log.warn("Fallo enviando notificación a session={}: {}", session.getId(), e.getMessage());
                removeSession(session);
            }
        }
    }

    private void removeSession(WebSocketSession session) {
        sessionsByUser.values().forEach(set -> set.removeIf(s -> s.getId().equals(session.getId())));
        sessionsByUser.entrySet().removeIf(e -> e.getValue().isEmpty());
    }
}