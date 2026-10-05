package cl.maraneda.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", LongDeserializer.class.getCanonicalName());
        props.setProperty("value.deserializer", StringDeserializer.class.getCanonicalName());
        props.setProperty("group.id", "TestGroup");

        try(KafkaConsumer<Long, String> cons = new KafkaConsumer<>(props)){
            cons.subscribe(List.of("topico1"));
            ConsumerRecords<Long, String> recs = cons.poll(Duration.ofSeconds(2400));
            recs.forEach(r -> System.out.format("Message received: Truck id = %d, coordinates = %s%n", r.key(), r.value()));
        }
    }
}
