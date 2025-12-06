package kafkafirehose

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	namespace = "kafkafirehose"
)

var (
	eventsReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "events_received",
	}, []string{"kind"})

	recordsHandled = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "records_handled",
	}, []string{"status", "collection"})

	messagesProduced = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "messages_produced",
	}, []string{"status"})
)
