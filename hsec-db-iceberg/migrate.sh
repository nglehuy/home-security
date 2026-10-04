#!/bin/sh
# Runs one Iceberg migration file with a local spark-sql 4.0.4.
#
# Before you run it, open two tunnels and keep them open:
#   kubectl -n hsec port-forward svc/postgres 15432:5432
#   kubectl -n hsec port-forward svc/rustfs-svc 19000:9000
#
# Usage, from the repository root:
#   hsec-db-iceberg/migrate.sh hsec-db-iceberg/migrations/0001_init.up.sql
#
# NAMESPACE, PG_PORT, and S3_PORT change the namespace and the local tunnel ports.
set -eu

if [ $# -ne 1 ] || [ ! -f "$1" ]; then
  echo "Usage: $0 <migration file>" >&2
  exit 2
fi
FILE=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
NAMESPACE=${NAMESPACE:-hsec}
PG_PORT=${PG_PORT:-15432}
S3_PORT=${S3_PORT:-19000}

secret() {
  kubectl -n "$NAMESPACE" get secret spark-env -o "jsonpath={.data.$1}" | base64 -d
}

# Spark writes metastore_db, derby.log, and spark-warehouse into the current
# folder, so it runs in a temporary folder outside the repository.
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
cd "$WORK"

# The passwords go into a file that only you can read, not onto the command line.
umask 077
cat > spark.conf <<CONF
spark.sql.extensions org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.lake org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lake.type jdbc
spark.sql.catalog.lake.uri jdbc:postgresql://localhost:$PG_PORT/iceberg_catalog
spark.sql.catalog.lake.jdbc.user iceberg
spark.sql.catalog.lake.jdbc.password $(secret POSTGRES_ICEBERG_PASSWORD)
spark.sql.catalog.lake.warehouse s3://hsec-lake/warehouse
spark.sql.catalog.lake.io-impl org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.lake.s3.endpoint http://localhost:$S3_PORT
spark.sql.catalog.lake.s3.path-style-access true
spark.sql.catalog.lake.s3.access-key-id $(secret RUSTFS_ACCESS_KEY)
spark.sql.catalog.lake.s3.secret-access-key $(secret RUSTFS_SECRET_KEY)
spark.sql.catalog.lake.client.region us-east-1
CONF

# The same jar versions as the hsec-app-spark image. The first run downloads them.
spark-sql --master "local[1]" \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.12.0,org.apache.iceberg:iceberg-aws-bundle:1.12.0,org.postgresql:postgresql:42.7.13 \
  --properties-file spark.conf \
  -f "$FILE"
