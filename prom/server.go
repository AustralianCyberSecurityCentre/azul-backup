package prom

import (
	"log"
	"net/http"

	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/settings"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var PrometheusScrapeCount uint64

type HandlerWrapper struct {
	handler http.Handler
}

func (h HandlerWrapper) ServeHTTP(rw http.ResponseWriter, rr *http.Request) {
	// Count number of times metrics are served.
	PrometheusScrapeCount += 1
	h.handler.ServeHTTP(rw, rr)
}

// Starts a HTTP server just for Prometheus - we don't want to setup the full dispatcher here
func StartStandalonePromServer() {
	handler := promhttp.Handler()
	http.Handle("/metrics", HandlerWrapper{handler: handler})
	bedSet.Logger.Info().Msg("launching metrics server")
	err := http.ListenAndServe(":8900", nil)
	if err != nil {
		log.Fatalf("failed to listen for prometheus metrics")
	}
}
