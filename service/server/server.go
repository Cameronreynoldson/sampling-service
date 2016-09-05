package server

import (
	"net/http"
)

type Server interface {
	Start() error
}

type serverImp struct {
	mux *http.ServeMux
}

// Constructor for the server. Initialize the server and
// http handlers.
func NewServer() Server {
	s := serverImp{
		mux: http.NewServeMux(),
	}

	// Handlers for the begin session, add integer and
	// end session APIs, respectively. See handlers.go
	// in reservoir/server for more information on the
	// handler constructors.
	s.mux.Handle("/begin", beginHandler())
	s.mux.Handle("/add", addHandler())
	s.mux.Handle("/end", endHandler())
	return s
}

// Start running the specified server.
func (s serverImp) Start() error {
	err := http.ListenAndServe(":8080", s.mux)
	return err
}
