#!/bin/bash
# thank you claude senpai *pleading emoji*
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}==>${NC} $1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}!${NC} $1"
}

cleanup() {
    print_warning "Shutting down services..."

    if [ ! -z "$DATABASE_PID" ]; then
        kill $DATABASE_PID 2>/dev/null || true
    fi
    if [ ! -z "$FIREHOSE_PID" ]; then
        kill $FIREHOSE_PID 2>/dev/null || true
    fi
    if [ ! -z "$INDEXER_PID" ]; then
        kill $INDEXER_PID 2>/dev/null || true
    fi
    if [ ! -z "$CDN_PID" ]; then
        kill $CDN_PID 2>/dev/null || true
    fi

    print_success "Services stopped"
    exit 0
}

trap cleanup SIGINT SIGTERM

print_status "Starting Docker Compose services (Zookeeper, Kafka, Cassandra)..."
docker compose up -d

if [ $? -ne 0 ]; then
    print_error "Failed to start Docker Compose services"
    exit 1
fi

print_success "Docker Compose services started"

print_status "Waiting for Cassandra to be ready..."
./scripts/setup-cassandra.sh

if [ $? -ne 0 ]; then
    print_error "Failed to setup Cassandra"
    exit 1
fi

print_success "Cassandra is ready and initialized"

print_status "Running database migrations..."
just migrate-up

if [ $? -ne 0 ]; then
    print_error "Failed to run migrations"
    exit 1
fi

print_success "Migrations completed"

print_status "Starting database server on :9090..."
go run ./cmd/database &
DATABASE_PID=$!
sleep 3

if ! ps -p $DATABASE_PID > /dev/null; then
    print_error "Database server failed to start"
    exit 1
fi

print_success "Database server running (PID: $DATABASE_PID)"

print_status "Starting firehose (connecting to Bluesky network)..."
go run ./cmd/bus/firehose --desired-collections "app.vylet.*" --websocket-host "wss://bsky.network" --output-topic firehose-events-prod &
FIREHOSE_PID=$!
sleep 3

if ! ps -p $FIREHOSE_PID > /dev/null; then
    print_error "Firehose failed to start"
    cleanup
    exit 1
fi

print_success "Firehose running (PID: $FIREHOSE_PID)"

print_status "Starting indexer (consuming from Kafka)..."
go run ./cmd/indexer &
INDEXER_PID=$!
sleep 3

if ! ps -p $INDEXER_PID > /dev/null; then
    print_error "Indexer failed to start"
    cleanup
    exit 1
fi

print_success "Indexer running (PID: $INDEXER_PID)"

print_status "Starting CDN (tracking blob references)..."
go run ./cmd/cdn &
CDN_PID=$!
sleep 3

if ! ps -p $CDN_PID > /dev/null; then
    print_error "CDN failed to start"
    cleanup
    exit 1
fi

print_success "CDN running (PID: $CDN_PID)"

echo ""
print_success "Full stack is running!"
echo ""
echo "Services:"
echo "  - Zookeeper:       localhost:2181"
echo "  - Kafka (broker 1): localhost:9092"
echo "  - Kafka (broker 2): localhost:9093"
echo "  - Kafka (broker 3): localhost:9094"
echo "  - Cassandra:       localhost:9042"
echo "  - Database Server: localhost:9090"
echo "  - Firehose:        PID $FIREHOSE_PID"
echo "  - Indexer:         PID $INDEXER_PID"
echo "  - CDN:             PID $CDN_PID"
echo ""
echo "Process IDs:"
echo "  - Database: $DATABASE_PID"
echo "  - Firehose: $FIREHOSE_PID"
echo "  - Indexer:  $INDEXER_PID"
echo "  - CDN:      $CDN_PID"
echo ""
print_warning "Press Ctrl+C to stop all services"
echo ""

wait
