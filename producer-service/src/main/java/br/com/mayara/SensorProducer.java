/**
* Classe que representa o produtor do chat.
*
* @author Maria Eduarda da Fonseca
* @version 1.0
* @since 2026-09-20
*/
package br.com.mayara;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.Date;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

///Json libs
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Map;

import java.net.InetAddress;

public class SensorProducer {

    private final KafkaProducer<String, String> producer; /// Instância do produtor Kafka para enviar mensagens.
    private static final Logger logger = LoggerFactory.getLogger(SensorProducer.class); /// Instância do logger para registrar informações e erros.
    private static final ObjectMapper mapper = new ObjectMapper();

    private final Random random = new Random();

    private final String topicTemperatura;
    private final String topicVibracao;
    private final String topicEnergia;
    private final String sensorId;
    private final String setor;
    private final long intervalMs;
    private final double anomalyProbability;
    private final double tempMin, tempMax;
    private final double vibMin, vibMax;
    private final double energiaMin, energiaMax;

    /**
     * Constructor for SensorProducer.
     *
     * @param topic The Kafka topic to which messages will be sent.
     */
    public SensorProducer() {
        
        this.topicTemperatura = System.getenv("TOPIC_TEMPERATURA");
        this.topicVibracao = System.getenv("TOPIC_VIBRACAO");
        this.topicEnergia = System.getenv("TOPIC_ENERGIA");

        //Pegando as variáveis de ambiente
        this.sensorId = hostname();
        this.setor = System.getenv("SENSOR_SETOR");
        this.intervalMs = (long) getEnvDouble("SENSOR_INTERVAL_MS");
        this.anomalyProbability = getEnvDouble("ANOMALY_PROBABILITY");
        this.tempMin = getEnvDouble("TEMP_MIN");
        this.tempMax = getEnvDouble("TEMP_MAX");
        this.vibMin = getEnvDouble("VIB_MIN");
        this.vibMax = getEnvDouble("VIB_MAX");
        this.energiaMin = getEnvDouble("ENERGIA_MIN");
        this.energiaMax = getEnvDouble("ENERGIA_MAX");

        //Setando as propriedades
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_BOOTSTRAP_SERVERS"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());

        ///Insere tolerância à falha do brocker
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // Evita duplicatas quando o producer reenvia após uma falha
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        this.producer = new KafkaProducer<>(props);
    }
     /**
     * Devolve o nome do host do container, usado como identificador do sensor.
     *
     * @return hostname do container
     */
    private static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "sensor-" + System.nanoTime();
        }
    }
    /**
     * Method to get numeric value of environment variable
     * @param name
     * @param defaultValue
     * @return
     */
    private static double getEnvDouble(String name) {
        String value = System.getenv(name);
        return Double.parseDouble(value);
    }
    /**
     * 
     * @param min
     * @param max
     * @return
     */
    private double getValue(double min, double max) {
        double valor;
        if (random.nextDouble() < this.anomalyProbability) {
            valor = max * 1.5;
        } else {
            valor = min + (max - min) * this.random.nextDouble();
        }
        return Math.round(valor * 10.0) / 10.0;
    }
    /**
     * Method that produces sensor data
     * @param type sensor's data type
     * @return
     */
    public String ProduceSensorData(double value) throws JsonProcessingException
    {
        ///sensor-id e setor pegar dos metadados??
       String json = mapper.writeValueAsString(Map.of(
                    "sensorId", this.sensorId,
                    "setor", this.setor,
                    "valor", value,
                    "timestamp", Instant.now().toString()
        ));
      
        return json;
    }
    /**
     * Sends a message to the Kafka topic.
     *
     * @param json_msg The message to be sent.
     * @param topic Topic that the message will be send off
     */
    public void sendMessage(String json_msg, String topic) {
        logger.info("[SensorProducer.sendMessage] " + json_msg);
        producer.send(new ProducerRecord<>(topic, sensorId, json_msg), (meta, ex) -> {
            if (ex != null) logger.error("[{}] falha ao enviar para {}", sensorId, topic, ex);
            else logger.info("[{}] enviado -> {} partição {} offset {}", sensorId, meta.topic(), meta.partition(), meta.offset());
        });
        //producer.send(new ProducerRecord<>(topic, sensorId, json_msg));
    }
    /**
     * Method that runs the sensors simulation
     */
    public void run() throws InterruptedException {
        while (true) {
            try {
                double value_temperatura = getValue(tempMin, tempMax);
                sendMessage(ProduceSensorData(value_temperatura), topicTemperatura);

                double value_vibracao = getValue(vibMin, vibMax);
                sendMessage(ProduceSensorData(value_vibracao), topicVibracao);

                double value_energia = getValue(energiaMin, energiaMax);
                sendMessage(ProduceSensorData(value_energia), topicEnergia);

            } catch (JsonProcessingException e) {
                logger.error("[{}] erro ao gerar JSON da leitura", sensorId, e);
            }
            Thread.sleep(intervalMs);
        }
    }
    /**
     * Closes the Kafka producer to release resources.
     */
    public void close() {
        producer.close();
        logger.info("[{}] sensor encerrado", sensorId);
    }

    /**
     * Main method to start the SensorProducer.
     *
     * @param args Command line arguments (not used).
     */
    public static void main(String[] args) throws InterruptedException{
        logger.info("[SensorProducer.main] Starting Sensor Producer.");
    
        SensorProducer sensorProducer = new SensorProducer();
        sensorProducer.run();
        
    }
}