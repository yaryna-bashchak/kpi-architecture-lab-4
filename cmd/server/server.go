package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/yaryna-bashchak/kpi-architecture-lab-4/httptools"
	"github.com/yaryna-bashchak/kpi-architecture-lab-4/signal"
)

var port = flag.Int("port", 8080, "server port")

const confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
const confHealthFailure = "CONF_HEALTH_FAILURE"

func main() {
	h := new(http.ServeMux)
	client := http.DefaultClient

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := make(Report)

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key != "" {
			resp, err := client.Get(fmt.Sprintf("http://db:8083/db/%s", key))
			statusOk := resp.StatusCode >= 200 && resp.StatusCode < 300
			if err != nil {
				log.Println(err)
				return
			}
			if !statusOk {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		report.Process(r)

		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		
		responseSize := 1024 
		if key == "" {
			if sizeHeader := r.Header.Get("Response-Size"); sizeHeader != "" {
				if size, err := strconv.Atoi(sizeHeader); err == nil && size > 0 {
					responseSize = size
				}
			}

			responseData := make([]string, responseSize)
			for i := 0; i < responseSize; i++ {
				responseData[i] = strconv.Itoa(responseSize)
			}

			_ = json.NewEncoder(rw).Encode(responseData)
		}

	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}
