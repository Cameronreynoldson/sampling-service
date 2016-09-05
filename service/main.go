package main

import (
	"service/server"
)

func main() {
	// Start the dispatcher and initialize workers.
	d := server.NewDispatcher(10)
	d.Start()
	// Start the server to handle http requests.
	s := server.NewServer()
	s.Start()
}
