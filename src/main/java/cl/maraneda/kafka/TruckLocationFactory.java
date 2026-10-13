package cl.maraneda.kafka;

import cl.maraneda.kafka.pojos.CardinalPoint;
import cl.maraneda.kafka.pojos.CoordinateEntry;
import cl.maraneda.kafka.pojos.TruckLocation;

public final class TruckLocationFactory {

    private TruckLocationFactory() {
    }

    public static TruckLocation createNorthEast(
            Long id,
            Double n1,
            Double n2) {

        return TruckLocation.newBuilder()
                .setId(id)
                .setLatitude(
                        CoordinateEntry.newBuilder()
                                .setNum(n1)
                                .setDir(
                                        n1 > 0
                                                ? CardinalPoint.N
                                                : CardinalPoint.S
                                )
                                .build()
                )
                .setLongitude(
                        CoordinateEntry.newBuilder()
                                .setNum(n2)
                                .setDir(
                                        n2 > 0
                                                ? CardinalPoint.E
                                                : CardinalPoint.W
                                )
                                .build()
                )
                .build();
    }

    public static TruckLocation createNorthWest(
            Long id,
            Double n1,
            Double n2) {

        return TruckLocation.newBuilder()
                .setId(id)
                .setLatitude(
                        CoordinateEntry.newBuilder()
                                .setNum(n1)
                                .setDir(
                                        n1 > 0
                                                ? CardinalPoint.N
                                                : CardinalPoint.S
                                )
                                .build()
                )
                .setLongitude(
                        CoordinateEntry.newBuilder()
                                .setNum(n2)
                                .setDir(
                                        n2 > 0
                                                ? CardinalPoint.W
                                                : CardinalPoint.E
                                )
                                .build()
                )
                .build();
    }

    public static TruckLocation createSouthEast(
            Long id,
            Double n1,
            Double n2) {

        return TruckLocation.newBuilder()
                .setId(id)
                .setLatitude(
                        CoordinateEntry.newBuilder()
                                .setNum(n1)
                                .setDir(
                                        n1 > 0
                                                ? CardinalPoint.S
                                                : CardinalPoint.N
                                )
                                .build()
                )
                .setLongitude(
                        CoordinateEntry.newBuilder()
                                .setNum(n2)
                                .setDir(
                                        n2 > 0
                                                ? CardinalPoint.E
                                                : CardinalPoint.W
                                )
                                .build()
                )
                .build();
    }

    public static TruckLocation createSouthWest(
            Long id,
            Double n1,
            Double n2) {

        return TruckLocation.newBuilder()
                .setId(id)
                .setLatitude(
                        CoordinateEntry.newBuilder()
                                .setNum(n1)
                                .setDir(
                                        n1 > 0
                                                ? CardinalPoint.S
                                                : CardinalPoint.N
                                )
                                .build()
                )
                .setLongitude(
                        CoordinateEntry.newBuilder()
                                .setNum(n2)
                                .setDir(
                                        n2 > 0
                                                ? CardinalPoint.W
                                                : CardinalPoint.E
                                )
                                .build()
                )
                .build();
    }
}