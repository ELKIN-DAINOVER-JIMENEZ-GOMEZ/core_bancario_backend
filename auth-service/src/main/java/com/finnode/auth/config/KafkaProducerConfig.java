package com.finnode.auth.config;

import com.finnode.auth.event.UserRegisteredEvent;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
public class KafkaProducerConfig {

	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;


	//Creamos un ProducerFactory para UserRegisteredEvent, que se encargará de crear productores de Kafka con la configuración especificada.
	@Bean
	public ProducerFactory<String, UserRegisteredEvent> userRegisteredProducerFactory() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);// Configuramos la dirección del servidor Kafka
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);// Configuramos el serializador para la clave del mensaje (String)
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);// Configuramos el serializador para el valor del mensaje (UserRegisteredEvent)
		props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);// Configuramos para no agregar información de tipo en los encabezados del mensaje
		return new DefaultKafkaProducerFactory<>(props);
	}

	@Bean
	public KafkaTemplate<String, UserRegisteredEvent> userRegisteredKafkaTemplate(
			ProducerFactory<String, UserRegisteredEvent> userRegisteredProducerFactory) {
		return new KafkaTemplate<>(userRegisteredProducerFactory);
	}
}