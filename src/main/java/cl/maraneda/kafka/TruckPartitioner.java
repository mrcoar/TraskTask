
package cl.maraneda.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class TruckPartitioner implements Partitioner {

    @Override
    public int partition(
            String topic,
            Object key,
            byte[] keyBytes,
            Object value,
            byte[] valueBytes,
            Cluster cluster) {

        List<PartitionInfo> partitions =
                cluster.availablePartitionsForTopic(topic);

        if (partitions.isEmpty()) {
            partitions = cluster.partitionsForTopic(topic);
        }

        if (partitions.isEmpty()) {
            throw new IllegalStateException(
                    "No hay particiones conocidas para el tópico: " + topic
            );
        }

        // Regla especial: la clave 1 va a la partición 2,
        // siempre que esa partición exista.
        if (key instanceof Number
                && ((Number) key).longValue() == 1L) {

            for (PartitionInfo partition : partitions) {
                if (partition.partition() == 2) {
                    return 2;
                }
            }

            throw new IllegalStateException(
                    "El tópico " + topic
                            + " no tiene disponible la partición 2"
            );
        }

        // Si no hay clave, utilizar una partición disponible.
        if (keyBytes == null) {
            return partitions.get(0).partition();
        }

        // Hash positivo y resultado dentro del rango válido.
        int index = Utils.toPositive(Utils.murmur2(keyBytes))
                % partitions.size();

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
