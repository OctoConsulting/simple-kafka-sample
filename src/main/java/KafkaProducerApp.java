import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

@Slf4j
public class KafkaProducerApp {
    private static final String topic = "test-topic";

    public static void main(String[] args) {
        System.out.println("Starting the producer main application");

        // Kafka Producer required properties
        //bootstrap.servers: Producer uses this to determine the cluster membership, partitions, leaders etc. You dont have to specify all the servers, but it's best practice to specify more than one broker, incase if the specified broker is unavailable the producer will use the next one.
        //key.serializer: class used for message serialization. This is to optimize the size of the messages not only for network transmission, but for storage and even compression
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer(props)) {
            int counter = 0;

            while (counter <= 5) {

                String msg = "{ \"message\" : \"test message" + counter + "\"}";
                ProducerRecord<String, String> message1 = new ProducerRecord<>(topic, msg);

                producer.send(message1);
                log.info("Sent message: " + msg);
                counter++;
            }

        } catch (Exception e) {
            log.error("Failed to send message by the producer", e);
        }
    }
}
