package com.finnode.payment.config;

import com.finnode.payment.websocket.PaymentNotificationHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.HandshakeInterceptor;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Map;

/**
 * Registra el endpoint WebSocket /ws/payments.
 * El navegador no puede enviar cabeceras en el handshake, así que el JWT llega
 * como query param: ws://host/ws/payments?token=<access_token>
 * y se valida aquí con el mismo JwtDecoder del Resource Server.
 */
@Configuration
@EnableWebSocket
@RequiredArgsConstructor
public class WebSocketConfig implements WebSocketConfigurer {

    private final PaymentNotificationHandler paymentNotificationHandler;
    private final JwtDecoder jwtDecoder;

    @Value("${websocket.allowed-origins:*}")
    private String[] allowedOrigins;

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(paymentNotificationHandler, "/ws/payments")
                .addInterceptors(new JwtHandshakeInterceptor(jwtDecoder))
                .setAllowedOriginPatterns(allowedOrigins);
    }

    @Slf4j
    @RequiredArgsConstructor
    static class JwtHandshakeInterceptor implements HandshakeInterceptor {

        private final JwtDecoder jwtDecoder;

        @Override
        public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response,
                                       WebSocketHandler wsHandler, Map<String, Object> attributes) {
            String token = UriComponentsBuilder.fromUri(request.getURI())
                    .build().getQueryParams().getFirst("token");

            if (token == null || token.isBlank()) {
                response.setStatusCode(HttpStatus.UNAUTHORIZED);
                return false;
            }
            try {
                Jwt jwt = jwtDecoder.decode(token);
                String userId = jwt.getClaimAsString("userId");
                if (userId == null) {
                    userId = jwt.getSubject();
                }
                if (userId == null) {
                    response.setStatusCode(HttpStatus.UNAUTHORIZED);
                    return false;
                }
                attributes.put(PaymentNotificationHandler.USER_ID_ATTRIBUTE, userId);
                return true;
            } catch (JwtException e) {
                log.warn("Handshake WebSocket rechazado: token inválido ({})", e.getMessage());
                response.setStatusCode(HttpStatus.UNAUTHORIZED);
                return false;
            }
        }

        @Override
        public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response,
                                   WebSocketHandler wsHandler, Exception exception) {
            // nada que hacer
        }
    }
}