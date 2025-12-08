package cdn

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	namespace = "cdn"
)

var (
	// Total number of blobs extracted from records
	blobsExtracted = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "blobs_extracted_total",
		Help:      "Total number of blobs extracted from firehose records",
	})

	// Database operations by operation type and status
	dbOperations = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "db_operations_total",
		Help:      "Total number of database operations",
	}, []string{"operation", "status"})

	// Records processed by operation type
	recordsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "records_processed_total",
		Help:      "Total number of records processed",
	}, []string{"operation"})
)
