package hsec.stream;

/** A message from frigate/reviews. Times are Unix seconds. */
public class ReviewMsg {
    public String type;
    public String id;
    public String camera;
    public String severity;
    public Double startTime;
    public Double endTime;
    public String[] detections;
    public String[] objects;
    public String[] subLabels;
    public String[] zones;

    public ReviewMsg() {}
}
