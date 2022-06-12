package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gorilla/mux"
)

// When building a JSON/HTTP Go server, each handler consists of threee steps:
// 1. Unmarshal the request's JSON body into a struct
// 2. Run the endpoint's logic with the request to obtain the result
// 3. Marshal and write that result to the response
//
// If your handlers become much more complicated that this, then you should
// move the code out, move request and response handling into HTTP middleware,
// and move business logic further down the stack.


type httpServer struct{
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request){
	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	off, err := s.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ProduceResponse{Offset: off}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request){
	var req ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	record, err := s.Log.Read(req.Offset)
	if err != nil {
		if errors.Is(err, ErrOffsetNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	res := ConsumeResponse{Record: record}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// ProduceRequest contains the record that the caller of our API
// wants appended to the log.
type ProduceRequest struct {
	Record Record `json:"record"`
}

// ProduceResponse tells the caller what offset the log stored the records under.
type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

// ConsumeRequest specifies which records the caller of our API wants to read.
type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

// ConsumeResponse to send back those records to the caller.
type ConsumeResponse struct {
	Record Record `json:"record"`
}


// NewHTTPServer takes in an address for the server to run
// and returns an *http.Server so the user just needs to call
// ListenAndServe() to listen for and handle incoming request.
func NewHTTPServer(addr string) *http.Server {
	httpsrv := newHTTPServer()
	r := mux.NewRouter()
	r.HandleFunc("/", httpsrv.handleProduce).Methods("POST")
	r.HandleFunc("/", httpsrv.handleConsume).Methods("GET")
	return &http.Server{
		Addr: addr,
		Handler: r,
	}
}