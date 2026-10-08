package cl.maraneda.kafka;

import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import org.apache.avro.generic.GenericRecord;
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
        props.setProperty("group.id", "TestGroup2");
        //props.setProperty("apicurio.registry.use-specific-avro-reader", "true");
        props.setProperty("apicurio.registry.url", "http://localhost:8081/apis/registry/v3");

        try(KafkaConsumer<Long, GenericRecord> cons = new KafkaConsumer<>(props)){
            cons.subscribe(List.of("topico1"));
            ConsumerRecords<Long, GenericRecord> recs = cons.poll(Duration.ofSeconds(2400));
            recs.forEach(r -> System.out.format("Message received: %s%n", r.toString()));
        }
    }
}
