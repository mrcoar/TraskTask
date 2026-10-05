package cl.maraneda.kafka;

public record CoordinateEntry(Double num, CardinalPoint dir) {

    public String toString(){
        return String.format("%f %c", num, dir.getChar());
    }

    public static CoordinateEntry createNorth(Double n){
        return new CoordinateEntry(n, n > 0 ? CardinalPoint.NORTH : CardinalPoint.SOUTH);
    }

    public static CoordinateEntry createSouth(Double n){
        return new CoordinateEntry(n, n > 0 ? CardinalPoint.SOUTH : CardinalPoint.NORTH);
    }

    public static CoordinateEntry createEast(Double n){
        return new CoordinateEntry(n, n > 0 ? CardinalPoint.EAST : CardinalPoint.WEST);
    }

    public static CoordinateEntry createWest(Double n){
        return new CoordinateEntry(n, n > 0 ? CardinalPoint.WEST : CardinalPoint.EAST);
    }
}
