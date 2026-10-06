package cl.maraneda.kafka;

import org.apache.kafka.common.serialization.Deserializer;
import tools.jackson.databind.ObjectMapper;

public class TruckLocationDeserializer implements Deserializer<TruckLocation> {
    @Override
    public TruckLocation deserialize(String topic, byte[] data) {
        System.out.println(new String(data));
        return new ObjectMapper().readValue(data, TruckLocation.class);
    }
}
