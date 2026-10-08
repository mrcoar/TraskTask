package cl.maraneda.kafka;

import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import io.apicurio.registry.serde.strategy.TopicIdStrategy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Producer {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", LongSerializer.class.getCanonicalName());
        props.setProperty("value.serializer", AvroKafkaSerializer.class.getName());
        props.setProperty("apicurio.registry.url", "http://localhost:8081/apis/registry/v3");
        props.setProperty(
                "apicurio.registry.artifact-resolver-strategy",
                TopicIdStrategy.class.getCanonicalName()
        );
        props.put("apicurio.registry.auto-register", "true");
        try (InputStream is = Producer.class
                .getClassLoader()
                .getResourceAsStream("truck_location.avsc")){
            Schema schema = new Schema.Parser().parse(is);
            List<ProducerRecord<Long, GenericRecord>> trucks = List.of(
                    new ProducerRecord<>("topico1", 1L, TruckLocation.toGenericRecord(TruckLocation.createNorthEast(1L, 20.1, 40.2), schema)),
                    new ProducerRecord<>("topico1", 2L, TruckLocation.toGenericRecord(TruckLocation.createNorthWest(2L, 25.1, 45.2), schema)),
                    new ProducerRecord<>("topico1", 3L, TruckLocation.toGenericRecord(TruckLocation.createSouthEast(3L, 30.1, 50.2), schema)),
                    new ProducerRecord<>("topico1", 4L, TruckLocation.toGenericRecord(TruckLocation.createSouthWest(4L, 35.1, 55.2), schema))
            );

            try(KafkaProducer<Long, GenericRecord> prod = new KafkaProducer<>(props)) {
                trucks.forEach(t -> {
                    System.out.println(t.value());
                    prod.send(t, (m, e) ->
                            System.out.format("Message with id %d sent %s%n", t.key(), e == null ? "successfully" : "with errors: " + e));
                    try {
                        Thread.sleep(2000L);
                    }catch(InterruptedException e){
                        IO.println("Proceso interrumpido");
                    }
                });
            }
        }
    }
}
