package e2e

import (
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/semistrict/loophole/metrics"
)

func TestMain(m *testing.M) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	go http.ListenAndServe(":9090", mux)
	fmt.Println("metrics available on :9090/metrics")
	os.Exit(m.Run())
}
