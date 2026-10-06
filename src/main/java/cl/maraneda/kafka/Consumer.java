package cl.maraneda.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", LongDeserializer.class.getCanonicalName());
        props.setProperty("value.deserializer", TruckLocationDeserializer.class.getCanonicalName());
        props.setProperty("group.id", "TestGroup2");

        try(KafkaConsumer<Long, TruckLocation> cons = new KafkaConsumer<>(props)){
            cons.subscribe(List.of("topico1"));
            ConsumerRecords<Long, TruckLocation> recs = cons.poll(Duration.ofSeconds(2400));
            recs.forEach(r -> System.out.format("Message received: %s%n", r.toString()));
        }
    }
}
