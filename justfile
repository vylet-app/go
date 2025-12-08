set shell := ["bash", "-cu"]

lexdir := "../lexicons"

default:
	@just --list

lexgen:
	go run ./cmd/lexgen/ --build-file cmd/lexgen/vylet.json {{lexdir}}

cborgen:
	go run ./gen

migrate-up:
    go run ./cmd/database/migrate -k vylet up

migrate-down:
    go run ./cmd/database/migrate -k vylet down

migrate-create name:
    #!/usr/bin/env bash
    timestamp=$(date +%s)
    touch migrations/${timestamp}_{{name}}.up.cql
    touch migrations/${timestamp}_{{name}}.down.cql
    echo "Created migrations/${timestamp}_{{name}}.up.cql"
    echo "Created migrations/${timestamp}_{{name}}.down.cql"

deps:
    go mod download
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

cassandra-setup:
    ./scripts/setup-cassandra.sh

cassandra-shell:
    docker exec -it cassandra cqlsh

run-database-server:
    go run ./cmd/database

run-firehose:
    go run ./cmd/bus/firehose --desired-collections "app.vylet.*" --websocket-host "wss://bsky.network" --output-topic firehose-events-prod

run-indexer:
    go run ./cmd/indexer

run-cdn:
    go run ./cmd/cdn

run-api:
    go run ./cmd/api

run-dev-env:
    bash dev.sh
