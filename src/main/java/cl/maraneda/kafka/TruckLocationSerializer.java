package cl.maraneda.kafka;

import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.databind.ObjectMapper;

public class TruckLocationSerializer implements Serializer<TruckLocation> {
    @Override
    public byte[] serialize(String topic, TruckLocation data) {
        return new ObjectMapper().writeValueAsString(data).getBytes();
    }
}
