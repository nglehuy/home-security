package hsec.stream;

/** A closed visit: a period of person activity on one camera. */
public class Visit {
    /** The camera name and the start time in milliseconds, for example front_door:1700000000123. */
    public String visitId;
    public String camera;
    public Double startTime;
    public Double endTime;
    public int personCount;
    public String[] knownNames;
    public int unknownCount;
    public String[] zones;
    public long ingestMs;

    public Visit() {}
}
