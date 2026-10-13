package cl.maraneda.kafka;

import cl.maraneda.kafka.pojos.TruckLocation;
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
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
        props.setProperty("value.deserializer", AvroKafkaDeserializer.class.getName());
        props.setProperty("group.id", "TestGroup3");
        props.setProperty("apicurio.registry.use-specific-avro-reader", "true");
        props.setProperty("apicurio.registry.url", "http://localhost:8081/apis/registry/v3");
        System.setProperty("org.apache.avro.SERIALIZABLE_PACKAGES","cl.maraneda.kafka.pojos");

        try(KafkaConsumer<Long, TruckLocation> cons = new KafkaConsumer<>(props)){
            cons.subscribe(List.of("topico1"));
            ConsumerRecords<Long, TruckLocation> recs = cons.poll(Duration.ofSeconds(2400));
            recs.forEach(r -> System.out.format("Message received: %s%n", r.toString()));
        }
    }
}
