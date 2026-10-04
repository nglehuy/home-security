-- Creates the five stats tables. The database hsec already exists, because the
-- ClickHouse pod sets CLICKHOUSE_DB=hsec.
--
-- Spark writes every row again on each run, with version set to the run start
-- time. ReplacingMergeTree keeps only the row with the highest version for each
-- order key, so a repeated run gives the same result. Dashboards query with FINAL.
-- A 29-day TTL drops whole daily partitions.

CREATE TABLE hsec.objects
(
    event_id    String,
    camera      LowCardinality(String),
    label       LowCardinality(String),
    known_name  Nullable(String),
    has_face    Bool,
    top_score   Float32,
    zones       Array(String),
    start_ts    DateTime64(3),
    end_ts      Nullable(DateTime64(3)),
    duration_s  Nullable(Float32),
    version     DateTime64(3)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toDate(start_ts)
ORDER BY (camera, start_ts, event_id)
TTL toDate(start_ts) + INTERVAL 29 DAY
SETTINGS ttl_only_drop_parts = 1;

CREATE TABLE hsec.visits
(
    visit_id      String,
    camera        LowCardinality(String),
    start_ts      DateTime64(3),
    end_ts        DateTime64(3),
    duration_s    Float32,
    person_count  Int32,
    known_names   Array(String),
    unknown_count Int32,
    zones         Array(String),
    version       DateTime64(3)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toDate(start_ts)
ORDER BY (camera, start_ts, visit_id)
TTL toDate(start_ts) + INTERVAL 29 DAY
SETTINGS ttl_only_drop_parts = 1;

CREATE TABLE hsec.alerts
(
    alert_id     String,
    review_id    String,
    camera       LowCardinality(String),
    severity     LowCardinality(String),
    objects      Array(String),
    sub_labels   Array(String),
    zones        Array(String),
    review_start DateTime64(3),
    event_id     Nullable(String),
    fired_ts     DateTime64(3),
    snapshot_url Nullable(String),
    version      DateTime64(3)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toDate(fired_ts)
ORDER BY (camera, fired_ts, alert_id)
TTL toDate(fired_ts) + INTERVAL 29 DAY
SETTINGS ttl_only_drop_parts = 1;

CREATE TABLE hsec.hourly_activity
(
    hour          DateTime,
    camera        LowCardinality(String),
    zone          LowCardinality(String),
    label         LowCardinality(String),
    objects       Int32,
    known_people  Int32,
    unknown_faces Int32,
    avg_top_score Float32,
    version       DateTime64(3)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toDate(hour)
ORDER BY (hour, camera, zone, label)
TTL toDate(hour) + INTERVAL 29 DAY
SETTINGS ttl_only_drop_parts = 1;

CREATE TABLE hsec.daily_stats
(
    day               Date,
    camera            LowCardinality(String),
    visits            Int32,
    people            Int32,
    faces_seen        Int32,
    unknown_faces     Int32,
    unknown_face_rate Float32,
    avg_score_day     Nullable(Float32),
    avg_score_night   Nullable(Float32),
    alerts            Int32,
    version           DateTime64(3)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY day
ORDER BY (day, camera)
TTL day + INTERVAL 29 DAY
SETTINGS ttl_only_drop_parts = 1;
