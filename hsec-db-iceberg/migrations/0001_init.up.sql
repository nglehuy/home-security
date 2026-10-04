-- Creates the namespace and the seven tables of the Iceberg catalog lake.
-- Spark SQL timestamp is Iceberg timestamptz, and array<string> is a list of strings.
-- A second run changes nothing: CREATE uses IF NOT EXISTS, and WRITE ORDERED BY
-- sets the same sort order again. WRITE ORDERED BY needs the Iceberg SQL extensions.

CREATE NAMESPACE IF NOT EXISTS lake.hsec;

-- One row per frigate/events message.
CREATE TABLE IF NOT EXISTS lake.hsec.events (
    message_id      string,
    event_id        string,
    msg_type        string,
    camera          string,
    label           string,
    sub_label       string,
    sub_label_score double,
    score           double,
    top_score       double,
    current_zones   array<string>,
    entered_zones   array<string>,
    face_score      double,
    has_snapshot    boolean,
    has_clip        boolean,
    start_ts        timestamp,
    end_ts          timestamp,
    event_ts        timestamp,
    ingest_ts       timestamp
)
USING iceberg
PARTITIONED BY (days(event_ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'copy-on-write'
);

ALTER TABLE lake.hsec.events WRITE ORDERED BY camera, event_ts;

-- One row per frigate/reviews message.
CREATE TABLE IF NOT EXISTS lake.hsec.reviews (
    message_id string,
    review_id  string,
    msg_type   string,
    camera     string,
    severity   string,
    start_ts   timestamp,
    end_ts     timestamp,
    detections array<string>,
    objects    array<string>,
    sub_labels array<string>,
    zones      array<string>,
    event_ts   timestamp,
    ingest_ts  timestamp
)
USING iceberg
PARTITIONED BY (days(event_ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'copy-on-write'
);

ALTER TABLE lake.hsec.reviews WRITE ORDERED BY camera, event_ts;

-- One row per frigate/tracked_object_update message.
CREATE TABLE IF NOT EXISTS lake.hsec.object_updates (
    message_id  string,
    event_id    string,
    update_type string,
    camera      string,
    name        string,
    score       double,
    plate       string,
    description string,
    model       string,
    sub_label   string,
    attribute   string,
    event_ts    timestamp,
    ingest_ts   timestamp
)
USING iceberg
PARTITIONED BY (days(event_ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'copy-on-write'
);

ALTER TABLE lake.hsec.object_updates WRITE ORDERED BY camera, event_ts;

-- One row per frigate/triggers message. event_ts is the bridge receive time.
CREATE TABLE IF NOT EXISTS lake.hsec.triggers (
    message_id string,
    event_id   string,
    camera     string,
    name       string,
    type       string,
    score      double,
    event_ts   timestamp,
    ingest_ts  timestamp
)
USING iceberg
PARTITIONED BY (days(event_ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'copy-on-write'
);

ALTER TABLE lake.hsec.triggers WRITE ORDERED BY camera, event_ts;

-- One row per message on a system topic. event_ts is the ingest time.
CREATE TABLE IF NOT EXISTS lake.hsec.system_messages (
    message_id string,
    topic      string,
    payload    string,
    event_ts   timestamp,
    ingest_ts  timestamp
)
USING iceberg
PARTITIONED BY (days(event_ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'copy-on-write'
);

ALTER TABLE lake.hsec.system_messages WRITE ORDERED BY topic, event_ts;

-- One row per closed visit.
CREATE TABLE IF NOT EXISTS lake.hsec.visits (
    visit_id      string,
    camera        string,
    start_ts      timestamp,
    end_ts        timestamp,
    person_count  int,
    known_names   array<string>,
    unknown_count int,
    zones         array<string>,
    event_ts      timestamp,
    ingest_ts     timestamp
)
USING iceberg
PARTITIONED BY (days(event_ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'copy-on-write'
);

ALTER TABLE lake.hsec.visits WRITE ORDERED BY camera, event_ts;

-- One row per alert message. image and clip hold the files, or null.
CREATE TABLE IF NOT EXISTS lake.hsec.alerts (
    alert_id      string,
    kind          string,
    review_id     string,
    camera        string,
    severity      string,
    objects       array<string>,
    sub_labels    array<string>,
    zones         array<string>,
    review_start  timestamp,
    event_id      string,
    fired_ts      timestamp,
    snapshot_url  string,
    part          int,
    part_count    int,
    clip_url      string,
    video_camera  string,
    video_from_ts timestamp,
    video_to_ts   timestamp,
    image         binary,
    clip          binary,
    event_ts      timestamp,
    ingest_ts     timestamp
)
USING iceberg
PARTITIONED BY (days(event_ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'copy-on-write'
);

ALTER TABLE lake.hsec.alerts WRITE ORDERED BY camera, event_ts;
