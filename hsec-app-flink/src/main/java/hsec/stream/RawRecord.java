package hsec.stream;

/** One Kafka record from a frigate.* topic, with its video headers if it has them. */
public class RawRecord {
    public String topic;
    public byte[] value;
    public String videoCamera;
    public Double videoFrom;
    public Double videoTo;

    public RawRecord() {}

    public RawRecord(String topic, byte[] value, String videoCamera, Double videoFrom, Double videoTo) {
        this.topic = topic;
        this.value = value;
        this.videoCamera = videoCamera;
        this.videoFrom = videoFrom;
        this.videoTo = videoTo;
    }
}
