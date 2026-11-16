// Package metrics exposes per-instance payment processor counters.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Counters struct {
	registry *prometheus.Registry
	results  map[string]prometheus.Counter
	dlq      prometheus.Counter
	retries  prometheus.Counter
}

func New() *Counters {
	registry := prometheus.NewRegistry()
	results := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "payment_processor_results_published_total",
		Help: "Payment results acknowledged by Kafka.",
	}, []string{"status"})
	dlq := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "payment_processor_dlq_published_total",
		Help: "Dead-letter messages acknowledged by Kafka.",
	})
	retries := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "payment_processor_processing_retries_total",
		Help: "Repeated handler attempts after transient failures.",
	})
	registry.MustRegister(results, dlq, retries)
	return &Counters{
		registry: registry,
		// Initialize both series at zero and admit only the final statuses in the contract.
		results: map[string]prometheus.Counter{
			"success": results.WithLabelValues("success"),
			"failed":  results.WithLabelValues("failed"),
		},
		dlq:     dlq,
		retries: retries,
	}
}

func (c *Counters) Handler() http.Handler {
	return promhttp.HandlerFor(c.registry, promhttp.HandlerOpts{})
}

func (c *Counters) ResultPublished(status string) {
	if counter := c.results[status]; counter != nil {
		counter.Inc()
	}
}

func (c *Counters) DeadLetterPublished() { c.dlq.Inc() }

func (c *Counters) ProcessingRetried() { c.retries.Inc() }
