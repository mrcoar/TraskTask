package cl.maraneda.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

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

    public static GenericRecord toGenericRecord(TruckLocation truck, Schema schema) {
        Schema coordinateSchema = schema.getField("latitude").schema();
        Schema cardinalPointSchema = coordinateSchema.getField("dir").schema();

        GenericRecord latitude = new GenericData.Record(coordinateSchema);
        latitude.put("num", truck.latitude().num());
        latitude.put(
                "dir",
                new GenericData.EnumSymbol(
                        cardinalPointSchema,
                        String.valueOf(truck.latitude().dir().getChar())
                )
        );

        GenericRecord longitude = new GenericData.Record(coordinateSchema);
        longitude.put("num", truck.longitude().num());
        longitude.put(
                "dir",
                new GenericData.EnumSymbol(
                        cardinalPointSchema,
                        String.valueOf(truck.longitude().dir().getChar())
                )
        );

        GenericRecord result = new GenericData.Record(schema);
        result.put("id", truck.id());
        result.put("latitude", latitude);
        result.put("longitude", longitude);
        return result;
    }
}
