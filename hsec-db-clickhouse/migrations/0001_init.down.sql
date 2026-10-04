-- Drops the five stats tables and their data. The tests use this file.
-- Do not run it on the cluster.
DROP TABLE IF EXISTS hsec.daily_stats;
DROP TABLE IF EXISTS hsec.hourly_activity;
DROP TABLE IF EXISTS hsec.alerts;
DROP TABLE IF EXISTS hsec.visits;
DROP TABLE IF EXISTS hsec.objects;
