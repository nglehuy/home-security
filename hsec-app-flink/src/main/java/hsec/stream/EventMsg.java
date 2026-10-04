package hsec.stream;

/** A message from frigate/events. Times are Unix seconds. */
public class EventMsg {
    public String type;
    public String id;
    public String camera;
    public String label;
    public String subLabel;
    public Double subLabelScore;
    public Double score;
    public Double topScore;
    public String[] currentZones;
    public String[] enteredZones;
    public Double faceScore;
    public Boolean hasSnapshot;
    public Boolean hasClip;
    public Double startTime;
    public Double endTime;
    public Double frameTime;

    public EventMsg() {}
}
