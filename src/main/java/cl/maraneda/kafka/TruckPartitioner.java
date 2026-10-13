
package cl.maraneda.kafka;

import cl.maraneda.kafka.pojos.TruckLocation;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class TruckPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
        if (partitions.isEmpty()) {
            partitions = cluster.partitionsForTopic(topic);
        }
        if (partitions.isEmpty()) {
            throw new IllegalStateException("No hay particiones conocidas para el tópico: " + topic);
        }

        if(value instanceof TruckLocation tl && tl.getLatitude().getNum() == 37.2431 && tl.getLongitude().getNum() == 115.793){
            return 5;
        }

        if (keyBytes == null) {
            return partitions.get(0).partition();
        }

        int index = Utils.toPositive(Utils.murmur2(keyBytes)) % partitions.size();
        return partitions.get(index).partition();
    }

    @Override
    public void close() {
        // No hay recursos que liberar.
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No requiere configuración adicional.
    }
}
