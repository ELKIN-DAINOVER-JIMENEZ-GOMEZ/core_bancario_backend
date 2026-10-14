package com.finnode.payment.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Habilita @Scheduled en el servicio (timeouts de la saga: fraude y ledger).
 */
@Configuration
@EnableScheduling
public class SchedulingConfig {
}