#!/usr/bin/env bash
# Operator evidence runner for object-log S3BlobStore.
#
# Usage:
#   ./scripts/s3-evidence.sh minio
#   ./scripts/s3-evidence.sh garage
#   OBJECT_LOG_S3_ENDPOINT=… OBJECT_LOG_S3_BUCKET=… \
#     OBJECT_LOG_S3_KEY_ID=… OBJECT_LOG_S3_SECRET=… \
#     OBJECT_LOG_S3_PROVIDER=aws ./scripts/s3-evidence.sh custom
#
# Prints a markdown evidence row on success. Never prints secrets.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PROVIDER="${1:-}"
if [[ -z "$PROVIDER" ]]; then
  cat <<'EOF'
Usage: s3-evidence.sh <minio|garage|custom|aws|r2>

  minio   — defaults to http://127.0.0.1:19000 (or :9000), minioadmin/minioadmin
  garage  — defaults to http://127.0.0.1:3900; requires KEY_ID/SECRET in env
  custom  — uses only OBJECT_LOG_S3_* from the environment
  aws/r2  — same as custom; sets OBJECT_LOG_S3_PROVIDER for the evidence row
EOF
  exit 2
fi

case "$PROVIDER" in
  minio)
    export OBJECT_LOG_S3_PROVIDER=minio
    export OBJECT_LOG_S3_ENDPOINT="${OBJECT_LOG_S3_ENDPOINT:-http://127.0.0.1:19000}"
    export OBJECT_LOG_S3_BUCKET="${OBJECT_LOG_S3_BUCKET:-object-log-evidence}"
    export OBJECT_LOG_S3_KEY_ID="${OBJECT_LOG_S3_KEY_ID:-minioadmin}"
    export OBJECT_LOG_S3_SECRET="${OBJECT_LOG_S3_SECRET:-minioadmin}"
    export OBJECT_LOG_S3_REGION="${OBJECT_LOG_S3_REGION:-us-east-1}"
    ;;
  garage)
    export OBJECT_LOG_S3_PROVIDER=garage
    export OBJECT_LOG_S3_ENDPOINT="${OBJECT_LOG_S3_ENDPOINT:-http://127.0.0.1:3900}"
    export OBJECT_LOG_S3_BUCKET="${OBJECT_LOG_S3_BUCKET:-object-log-evidence}"
    export OBJECT_LOG_S3_REGION="${OBJECT_LOG_S3_REGION:-garage}"
    if [[ -z "${OBJECT_LOG_S3_KEY_ID:-}" || -z "${OBJECT_LOG_S3_SECRET:-}" ]]; then
      echo "error: set OBJECT_LOG_S3_KEY_ID and OBJECT_LOG_S3_SECRET for garage" >&2
      exit 1
    fi
    ;;
  custom|aws|r2)
    export OBJECT_LOG_S3_PROVIDER="${OBJECT_LOG_S3_PROVIDER:-$PROVIDER}"
    for v in OBJECT_LOG_S3_ENDPOINT OBJECT_LOG_S3_BUCKET OBJECT_LOG_S3_KEY_ID OBJECT_LOG_S3_SECRET; do
      if [[ -z "${!v:-}" ]]; then
        echo "error: $v is required for provider=$PROVIDER" >&2
        exit 1
      fi
    done
    export OBJECT_LOG_S3_REGION="${OBJECT_LOG_S3_REGION:-us-east-1}"
    ;;
  *)
    echo "error: unknown provider '$PROVIDER'" >&2
    exit 2
    ;;
esac

DATE="$(date -u +%Y-%m-%d)"
HOST="$(uname -n 2>/dev/null || echo unknown)"
echo "== object-log S3 evidence =="
echo "date_utc=$DATE host=$HOST provider=$OBJECT_LOG_S3_PROVIDER"
echo "endpoint=$OBJECT_LOG_S3_ENDPOINT bucket=$OBJECT_LOG_S3_BUCKET region=$OBJECT_LOG_S3_REGION"
echo "(credentials not printed)"
echo

cargo test --features s3 --test s3 -- --nocapture

echo
echo "### Evidence row (paste into docs/helix/02-design/technical-designs/TD-002-…md)"
echo "| $DATE | ${OBJECT_LOG_S3_PROVIDER} @ \`${OBJECT_LOG_S3_ENDPOINT}\` (bucket \`${OBJECT_LOG_S3_BUCKET}\`) | \`s3_blob_store_round_trip\` + \`s3_multipart_put_get_range_round_trip\` + \`s3_engine_produce_fetch_round_trip\` green |"
