package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/yaryna-bashchak/kpi-architecture-lab-4/httptools"
	"github.com/yaryna-bashchak/kpi-architecture-lab-4/signal"
)

var (
	port = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

type Server struct {
	URL     string
	ConnCnt int32
	Healthy int32
}

var (
	timeout = time.Duration(*timeoutSec) * time.Second
	serversPool = []*Server{
		{URL: "server1:8080"},
		{URL: "server2:8080"},
		{URL: "server3:8080"},
	}
)

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func Health(server *Server) bool {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/Health", scheme(), server.URL), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		atomic.StoreInt32(&server.Healthy, 0)
		return false
	}
	if resp.StatusCode != http.StatusOK {
		atomic.StoreInt32(&server.Healthy, 0)
		return false
	}
	atomic.StoreInt32(&server.Healthy, 1)
	return true
}

func FindMinServer() int {
	minServerIndex := -1
	minServerConnCnt := int32(math.MaxInt32)

	for i, server := range serversPool {
		if atomic.LoadInt32(&server.Healthy) == 1 {
			if minServerIndex == -1 || atomic.LoadInt32(&server.ConnCnt) < minServerConnCnt {
				minServerIndex = i
				minServerConnCnt = atomic.LoadInt32(&server.ConnCnt)
			}
		}
	}

	return minServerIndex
}

func forward(rw http.ResponseWriter, r *http.Request) error {
	ctx, _ := context.WithTimeout(r.Context(), timeout)
	fwdRequest := r.Clone(ctx)

	minServerIndex := FindMinServer()

	if minServerIndex == -1 {
		rw.WriteHeader(http.StatusServiceUnavailable)
		return fmt.Errorf("all servers are busy")
	}

	dst := serversPool[minServerIndex]
	atomic.AddInt32(&dst.ConnCnt, 1)
	defer atomic.AddInt32(&dst.ConnCnt, -1)

	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst.URL
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst.URL

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", dst.URL)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst.URL, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func main() {
	flag.Parse()

	for _, server := range serversPool {
		Health(server)
		go func(s *Server) {
			for range time.Tick(10 * time.Second) {
				Health(s)
				log.Printf("%s: Health=%t, connCnt=%d", s.URL, atomic.LoadInt32(&s.Healthy) == 1, atomic.LoadInt32(&s.ConnCnt))
			}
		}(server)
	}

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		forward(rw, r)
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}
