package server

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
)

type requestData struct {
	// Initial reservoir data passed in for reservoir sampling.
	Reservoir []int `json:"reservoir"`
	// Session ID for each reservoir sampling session.
	SessionID uint64 `json:"session"`
	// Integer that we will add to an existing reservoir using
	// the "/add" api.
	IntegerToAdd int `json:"add"`
	// ResponseWriter used for setting the status codes and
	// sending responses back to the user. We store this here
	// so that we can package the response during job processing,
	// instead of in the handler.
	responseWriter http.ResponseWriter
}

type responseData struct {
	// Result of reservoir sampling.
	Sample []int `json:"sample"`
}

const (
	MISSING_INPUT_SESSION_ID = ""
	MISSING_INPUT_RESERVOIR  = 0
	MISSING_INPUT_ADD        = ""
)

// Constructor for the handler that will handle
// "begin session" requests, which take in a list of K
// integers.
func beginHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req requestData
		body, _ := ioutil.ReadAll(r.Body)
		err := json.Unmarshal(body, &req)
		req.responseWriter = w

		// Handle invalid input.
		if err != nil {
			http.Error(w, "Invalid input: make sure the input matches specifications.",
				http.StatusBadRequest)
		}
		// Missing input.
		if len(req.Reservoir) == MISSING_INPUT_RESERVOIR {
			http.Error(w, "Missing input: initial K integers for reservoir.",
				http.StatusBadRequest)
		}

		// Start a new job and add it to the job queue.
		work, done := NewJob(req, JOB_BEGIN_SESSION)
		JobQueue <- work
		<-done
	})
}

// Constructor for the handler that will handle
// "add integer" requests, which take in a single integer
// and add the integer to an existing reservoir.
func addHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req requestData
		body, _ := ioutil.ReadAll(r.Body)
		err := json.Unmarshal(body, &req)
		req.responseWriter = w
		// Handle invalid input.
		if err != nil {
			http.Error(w, "Invalid input: make sure the input matches specifications.",
				http.StatusBadRequest)
		}

		// Start a new job and add it to the job queue.
		work, done := NewJob(req, JOB_ADD_INTEGER)
		JobQueue <- work
		<-done
	})
}

// Constructor for the handler that will handle
// "end session" requests, which will return a list
// of K integers.
func endHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set the content type to JSON so that the user is aware that they
		// are getting a JSON response.
		w.Header().Set("Content-Type", "application/json")

		var req requestData
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading request body.",
				http.StatusInternalServerError)
		}
		req.responseWriter = w

		err = json.Unmarshal(body, &req)
		// Handle invalid input.
		if err != nil {
			http.Error(w, "Invalid input: make sure the input matches specifications.",
				http.StatusBadRequest)
		}

		// Start a new job and add it to the job queue.
		work, done := NewJob(req, JOB_END_SESSION)
		JobQueue <- work
		<-done
	})
}
