package hsec.stream;

/** One parsed message. Exactly one of the message fields is set, depending on kind. */
public class Envelope {
    public static final String EVENT = "event";
    public static final String REVIEW = "review";
    public static final String OBJECT_UPDATE = "object_update";
    public static final String TRIGGER = "trigger";
    public static final String SYSTEM = "system";

    public String kind;
    /** SHA-256 of the topic and the raw bytes, in hex. */
    public String messageId;
    public String topic;
    public long ingestMs;

    public EventMsg event;
    public ReviewMsg review;
    public ObjectUpdateMsg update;
    public TriggerMsg trigger;
    public SystemMsg system;

    public Envelope() {}
}
