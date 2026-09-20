/**
* ChatConsumer.java
* This class implements a Kafka consumer that listens for messages on the "chatmessages" topic.
* It receives messages from Kafka and broadcasts them to all connected WebSocket
clients.
*
* @author Meslin
* @version 1.0
* @since 2024-06-10
*/
package br.com.mayara;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ChatConsumer {
    private static final String TOPIC = System.getenv("KAFKA_TOPIC"); /// Nome do tópico Kafka do chat.
    private static final String GROUP_ID = System.getenv("KAFKA_GROUP_ID"); /// ID do grupo de consumidores Kafka.
    private static final Logger logger = LoggerFactory.getLogger(ChatConsumer.class);

    /// Instância do logger para registrar informações e erros.
    /**
     * Main method to start the ChatConsumer and WebSocket server.
     * It initializes the Kafka consumer, subscribes to the "chat-messages" topic,
     * and continuously polls for new messages.
     * Received messages are broadcasted to all connected WebSocket clients.
     *
     * @param args Command line arguments (not used).
     */
    public static void main(String[] args) {
        logger.info("Starting Chat Consumer.");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_BOOTSTRAP_SERVERS"));
        props.put("group.id", GROUP_ID);
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        double sensorMin = Double.parseDouble(System.getenv("SENSOR_MIN"));
        double sensorMax = Double.parseDouble(System.getenv("SENSOR_MAX"));
        ObjectMapper mapper = new ObjectMapper();

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        JsonNode json = mapper.readTree(record.value());
                        JsonNode valueNode = json.get(TOPIC);

                        if (valueNode == null || !valueNode.isNumber()) {
                            logger.error(
                                "Campo '{}' ausente ou inválido na mensagem: {}",
                                TOPIC,
                                record.value()
                            );
                            continue;
                        }

                        int sensorId = json.get("sensorID").asInt();
                        String setor = json.get("setor").asText();
                        double sensorValue = valueNode.asDouble();
                        String timestamp = json.get("timestamp").asText();


                        if (sensorValue < sensorMin || sensorValue > sensorMax) {
                            logger.warn("Valor fora do limite: {}", sensorValue);
                        } else {
                            logger.info("Valor dentro do limite: {}", sensorValue);
                        }

                        logger.info(
                            "Sensor ID: {} | Setor: {} | {}: {} | Timestamp: {}",
                            sensorId,
                            setor,
                            TOPIC,
                            sensorValue,
                            timestamp
                        );
                    } catch (JsonProcessingException exception) {
                        logger.error("JSON inválido recebido: {}", record.value());
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
}
