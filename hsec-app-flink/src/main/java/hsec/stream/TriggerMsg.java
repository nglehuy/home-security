package hsec.stream;

/** A message from frigate/triggers. It has no time, so receiveTime is the bridge receive time. */
public class TriggerMsg {
    public String eventId;
    public String camera;
    public String name;
    public String type;
    public Double score;
    public Double receiveTime;

    public TriggerMsg() {}
}
