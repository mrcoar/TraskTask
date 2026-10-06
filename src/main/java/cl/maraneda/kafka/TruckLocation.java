package cl.maraneda.kafka;

public record TruckLocation(Long id, CoordinateEntry latitude, CoordinateEntry longitude) {
    public static TruckLocation createNorthWest(Long id, Double n1, Double n2){
        return new TruckLocation(id, CoordinateEntry.createNorth(n1), CoordinateEntry.createWest(n2));
    }

    public static TruckLocation createNorthEast(Long id, Double n1, Double n2){
        return new TruckLocation(id, CoordinateEntry.createNorth(n1), CoordinateEntry.createEast(n2));
    }

    public static TruckLocation createSouthWest(Long id, Double n1, Double n2){
        return new TruckLocation(id, CoordinateEntry.createSouth(n1), CoordinateEntry.createWest(n2));
    }

    public static TruckLocation createSouthEast(Long id, Double n1, Double n2){
        return new TruckLocation(id, CoordinateEntry.createSouth(n1), CoordinateEntry.createEast(n2));
    }
}
