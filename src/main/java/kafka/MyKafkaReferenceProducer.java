package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class MyKafkaReferenceProducer {

    public final static String TOPIC = "flink-table-reference";

    public final static List<String> CURRENCIES = Arrays.asList("USD", "EUR", "NOK", "DKK");
    final static Random rand = new Random();


    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        while (true) {

            for (String currency : CURRENCIES) {

                float rate = rand.nextFloat() + 1;

                JSONObject value = new JSONObject();
                value.put("currency", currency);
                value.put("rate", rate);

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, value.toString());
                producer.send(record);
            }

            Thread.sleep(2000);
        }
    }
}
