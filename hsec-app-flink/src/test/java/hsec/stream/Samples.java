package hsec.stream;

import java.nio.charset.StandardCharsets;

/** Test messages, shaped like the examples in the Frigate MQTT documentation. */
final class Samples {
    private Samples() {}

    static final String EVENT_NEW = """
            {"type":"new","before":{},"after":{"id":"1607123955.475377-mxklsc","camera":"front_door","frame_time":1607123961.837752,\
            "snapshot":null,"label":"person","sub_label":["John",0.91],"top_score":0.958984375,"false_positive":false,\
            "start_time":1607123955.475377,"end_time":null,"score":0.7890625,"box":[424,500,536,712],"area":23744,"ratio":2.113207,\
            "region":[264,450,667,853],"current_zones":["driveway"],"entered_zones":["yard","driveway"],"thumbnail":null,\
            "has_snapshot":false,"has_clip":false,"active":true,"stationary":false,"motionless_count":0,"position_changes":2,\
            "attributes":{"face":0.86}}}""";

    static final String REVIEW_NEW = """
            {"type":"new","before":{},"after":{"id":"1718987129.308396-fqk5ka","camera":"front_cam","start_time":1718987129.308396,\
            "end_time":null,"severity":"alert","thumb_path":"/media/frigate/clips/review/thumb-front_cam-1718987129.308396-fqk5ka.webp",\
            "data":{"detections":["1718987128.947436-g92ztx","1718987148.879516-d7oq7r"],"objects":["person","car"],\
            "sub_labels":[],"zones":["front_yard"],"audio":[]}}}""";

    static final String FACE_UPDATE = """
            {"type":"face","id":"1607123955.475377-mxklsc","name":"John","score":0.95,"camera":"front_door_cam","timestamp":1607123958.748393}""";

    static final String DESCRIPTION_UPDATE = """
            {"type":"description","id":"1607123955.475377-mxklsc","description":"The car is a red sedan moving away from the camera."}""";

    static final String TRIGGER = """
            {"name":"car_trigger","camera":"driveway","event_id":"1752857206.193062-xyz","type":"thumbnail","score":0.85}""";

    static RawRecord raw(String topic, String body) {
        return new RawRecord(topic, body.getBytes(StandardCharsets.UTF_8), null, null, null);
    }
}
