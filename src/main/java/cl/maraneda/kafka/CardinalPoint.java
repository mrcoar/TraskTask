package cl.maraneda.kafka;

public enum CardinalPoint {
    NORTH('N'), SOUTH('S'), EAST('E'), WEST('W');

    private final char cpchar;

    private CardinalPoint(char cpchar){
        this.cpchar = cpchar;
    }

    public char getChar(){
        return cpchar;
    }
}
