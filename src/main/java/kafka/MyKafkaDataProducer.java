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

import static kafka.MyKafkaReferenceProducer.CURRENCIES;

public class MyKafkaDataProducer {

    public final static String TOPIC = "flink-table-orders";

    final static Random rand = new Random();

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        int i = 0;
        while (true) {

            JSONObject value = new JSONObject();
            value.put("order_id", i++);
            value.put("currency", CURRENCIES.get( i % CURRENCIES.size()));
//            value.put("currency", "USD");
            value.put("order_value", rand.nextInt(100));

            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, value.toString());
            producer.send(record);

            Thread.sleep(500);
        }
    }
}
