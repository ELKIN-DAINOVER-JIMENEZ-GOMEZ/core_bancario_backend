package com.finnode.payment.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuración del consumer del orquestador.
 *
 * Los mensajes llegan como String y StringJsonMessageConverter los convierte al tipo del
 * parámetro del @KafkaListener (no depende de cabeceras de tipo), por eso también sirve para
 * eventos de servicios que no son Java, como fraud-engine (Python).
 *
 * Topics consumidos (ver PaymentEventConsumer):
 *  - fraud.check-completed            (fraud-engine)
 *  - account.funds-reserved / account.funds-reservation-failed / account.funds-released
 *  - ledger.entries-recorded / ledger.recording-failed
 */
@EnableKafka
@Configuration
public class KafkaConsumerConfig {

	@Value("${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}")
	private String bootstrapServers;

	@Value("${SPRING_KAFKA_CONSUMER_GROUP_ID:payment-orchestrator-group}")
	private String groupId;

	@Bean
	public ConsumerFactory<String, String> consumerFactory() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return new DefaultKafkaConsumerFactory<>(props);
	}

	/**
	 * Cambios para fraud-engine y WebSocket:
	 *  - FAIL_ON_UNKNOWN_PROPERTIES desactivado: si fraud-engine agrega campos al evento
	 *    (ej. nuevas reglas), el orquestador no rechaza el mensaje.
	 *  - WRITE_DATES_AS_TIMESTAMPS desactivado: Instant/LocalDateTime se escriben en ISO-8601
	 *    (en el WebSocket y en las respuestas REST) en lugar de números/arrays.
	 */
	@Bean("kafkaObjectMapper")
	public ObjectMapper kafkaObjectMapper() {
		return JsonMapper.builder()
				.findAndAddModules()
				.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
				.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
				.build();
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
			ConsumerFactory<String, String> consumerFactory,
			@Qualifier("kafkaObjectMapper") ObjectMapper kafkaObjectMapper) {

		ConcurrentKafkaListenerContainerFactory<String, String> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory);
		factory.setRecordMessageConverter(new StringJsonMessageConverter(kafkaObjectMapper));
		return factory;
	}
}