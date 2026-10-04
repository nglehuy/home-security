package hsec.stream;

/** A request to the media step: the snapshot for message 1, or the clip for one window. */
public class AlertRequest {
    public static final String START = "start";
    public static final String CLIP = "clip";

    public String alertId;
    public String kind;
    public String reviewId;
    public String camera;
    public String severity;
    public String[] objects;
    public String[] subLabels;
    public String[] zones;
    public double reviewStart;
    /** The tracked object for the snapshot. Null for clips, or when the review has no detections. */
    public String eventId;
    public long firedMs;
    public int part;
    public int partCount;
    /** The video window, in Unix seconds. For message 1, both are the review start. */
    public double from;
    public double to;
    /** Processing time when the clip window is ready to download. */
    public long readyMs;

    public AlertRequest() {}
}
