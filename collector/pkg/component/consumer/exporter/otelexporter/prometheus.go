package otelexporter

import (
	"net/http"
	"os"

	"github.com/Kindling-project/kindling/collector/pkg/component"
	"go.opentelemetry.io/otel/exporters/prometheus"
)

func StartServer(exporter *prometheus.Exporter, telemetry *component.TelemetryTools, ports map[string]string) error {
	http.HandleFunc("/metrics", exporter.ServeHTTP)
	var port string
	kindling_mod_number := os.Getenv("kindling_mod_number")
	if kindling_mod_number == "" {
		telemetry.Logger.Panic("kindling_mod_number environment variable is not set")
	}
	for k, v := range ports {
		if k == kindling_mod_number {
			port = v
			break
		}
	}
	if port == "" {
		telemetry.Logger.Panic("Can't find prometheus metric prot")
	}
	srv := http.Server{
		Addr:    port,
		Handler: http.DefaultServeMux,
	}

	telemetry.Logger.Infof("Prometheus Server listening at port: [%s]", port)
	err := srv.ListenAndServe()

	if err != nil && err != http.ErrServerClosed {
		return err
	}

	telemetry.Logger.Infof("Prometheus gracefully shutdown the http server...\n")

	return nil
}
