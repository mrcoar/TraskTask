package cl.maraneda.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.List;
import java.util.Properties;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Producer {
    static void main() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", LongSerializer.class.getCanonicalName());
        props.setProperty("value.serializer", TruckLocationSerializer.class.getCanonicalName());

        List<ProducerRecord<Long, TruckLocation>> trucks = List.of(
            new ProducerRecord<>("topico1", 1L, TruckLocation.createNorthEast(1L, 20.1, 40.2)),
            new ProducerRecord<>("topico1", 2L, TruckLocation.createNorthWest(2L, 25.1, 45.2)),
            new ProducerRecord<>("topico1", 3L, TruckLocation.createSouthEast(3L, 30.1, 50.2)),
            new ProducerRecord<>("topico1", 4L, TruckLocation.createSouthWest(4L, 35.1, 55.2))
        );

        try(KafkaProducer<Long, TruckLocation> prod = new KafkaProducer<>(props)) {
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
