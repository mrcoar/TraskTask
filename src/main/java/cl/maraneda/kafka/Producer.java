package cl.maraneda.kafka;

import cl.maraneda.kafka.pojos.TruckLocation;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import io.apicurio.registry.serde.strategy.TopicIdStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.List;
import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", LongSerializer.class.getCanonicalName());
        props.setProperty("value.serializer", AvroKafkaSerializer.class.getName());
        props.setProperty("apicurio.registry.url", "http://localhost:8081/apis/registry/v3");
        props.setProperty("apicurio.registry.auto-register", "true");
        props.setProperty("apicurio.registry.artifact-resolver-strategy", TopicIdStrategy.class.getCanonicalName() );
        props.setProperty("partitioner.class", TruckPartitioner.class.getName());
        System.setProperty("org.apache.avro.SERIALIZABLE_PACKAGES","cl.maraneda.kafka.pojos");

        List<ProducerRecord<Long, TruckLocation>> trucks = List.of(
                new ProducerRecord<>("topico4", 1L, TruckLocationFactory.createNorthEast(1L, 20.1, 40.2)),
                new ProducerRecord<>("topico4", 2L, TruckLocationFactory.createNorthWest(2L, 25.1, 45.2)),
                new ProducerRecord<>("topico4", 3L, TruckLocationFactory.createSouthEast(3L, 30.1, 50.2)),
                new ProducerRecord<>("topico4", 4L, TruckLocationFactory.createSouthWest(4L, 35.1, 55.2))
        );

        try (KafkaProducer<Long, TruckLocation> producer = new KafkaProducer<>(props)) {
            trucks.forEach(t -> {
                producer.send(t, (metadata, exception) -> {
                    System.out.printf("Message with id %d sent %s%n", t.key(), exception == null ? "successfully" : "with errors: " + exception);
                });

                try {
                    Thread.sleep(2000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.println("Proceso interrumpido");
                }
            });
        }
    }
}