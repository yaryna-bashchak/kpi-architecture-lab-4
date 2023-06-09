package main

import (
	"flag"
	"log"
	"net/http"
	"strconv"

	"github.com/yaryna-bashchak/kpi-architecture-lab-4/httptools"
	"github.com/yaryna-bashchak/kpi-architecture-lab-4/signal"
)

var port = flag.Int("port", 8083, "server port")

type server struct {
	*http.ServeMux
}

func main() {
	flag.Parse()

	s := &server{ServeMux: http.NewServeMux()}
	s.HandleFunc("/db/", handleDBRequest)

	httpServer := httptools.CreateServer(*port, s)
	httpServer.Start()

	signal.WaitForTerminationSignal()
}

func (s *server) Start() {
	log.Printf("Server listening on port %d", *port)
	err := http.ListenAndServe(":"+strconv.Itoa(*port), s)
	if err != nil {
		log.Fatal(err)
	}
}

func handleDBRequest(res http.ResponseWriter, req *http.Request) {
	log.Println("Caught request")
	key := req.URL.Path[len("/db/"):]
	log.Printf("Key: %s", key)

	switch req.Method {
	case http.MethodGet:
		log.Println("Caught GET request")
		res.WriteHeader(http.StatusOK)
	case http.MethodPost:
		log.Println("Caught POST request")
		res.WriteHeader(http.StatusCreated)
	default:
		res.WriteHeader(http.StatusBadRequest)
	}
}