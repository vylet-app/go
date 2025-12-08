# Vylet Go Monorepo

## Services

### CDN Service

The CDN service tracks blob references from the ATProto firehose and stores them in the database for resolution and serving.

#### What it does

- Subscribes to the Kafka firehose topic
- Extracts blob references from all records using `atdata.ExtractBlobs()`
- Stores blob metadata in the `blob_refs` table with DID and CID as primary key
- Tracks first seen time, processing time, and update time for each blob
- Exposes Prometheus metrics for monitoring

#### Running locally

```bash
# Using justfile
just run-cdn

# Or directly with go run
go run ./cmd/cdn
```

#### Configuration

Environment variables:
- `VYLET_CDN_DATABASE_HOST` - Database server address (default: `127.0.0.1:9090`)
- `VYLET_BOOTSTRAP_SERVERS` - Kafka bootstrap servers (default: `localhost:9092`)
- `VYLET_CDN_INPUT_TOPIC` - Firehose topic to consume (default: `firehose-events-prod`)
- `VYLET_CDN_CONSUMER_GROUP` - Kafka consumer group (required)

#### Metrics

The CDN service exposes the following Prometheus metrics:

- `cdn_blobs_extracted_total` - Total number of blobs extracted from records
- `cdn_db_operations_total{operation, status}` - Database operations by type (create/update) and status (success/error)
- `cdn_records_processed_total{operation}` - Records processed by operation type

#### API Integration

Blob references tracked by the CDN service can be resolved via the API:

```
GET /xrpc/app.vylet.media.getBlob?did=<did>&cid=<cid>
```

This endpoint:
1. Fetches blob metadata from `blob_refs` table
2. Checks if the blob is taken down
3. Resolves the PDS endpoint from the DID document
4. Returns a 302 redirect to the blob on the user's PDS
