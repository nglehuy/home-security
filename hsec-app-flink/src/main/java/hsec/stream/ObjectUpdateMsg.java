package hsec.stream;

/** A message from frigate/tracked_object_update. A field that the update type does not have is null. */
public class ObjectUpdateMsg {
    public String type;
    public String id;
    public String camera;
    public String name;
    public Double score;
    public String plate;
    public String description;
    public String model;
    public String subLabel;
    public String attribute;
    public Double timestamp;

    public ObjectUpdateMsg() {}
}
