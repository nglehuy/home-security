package hsec.stream;

/** A message from a Frigate system topic. The payload is the raw text. */
public class SystemMsg {
    public String payload;

    public SystemMsg() {}

    public SystemMsg(String payload) {
        this.payload = payload;
    }
}
