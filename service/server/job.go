package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"service/api"
)

// Enum for the type of jobs that we will process.
type JobType int

var JobQueue chan Job

const (
	JOB_BEGIN_SESSION = 1
	JOB_ADD_INTEGER   = 2
	JOB_END_SESSION   = 3
)

type Job interface {
	Process() error
	markDone()
}

// Job type for the /begin endpoint.
type JobBeginSession struct {
	request requestData
	// done will be used to determine when a job
	// has been processed.
	done chan bool
}

// Job type for the /add endpoint.
type JobAddInteger struct {
	request requestData
	// done will be used to determine when a job
	// has been processed.
	done chan bool
}

// Job type for the /end endpoint.
type JobEndSession struct {
	request requestData
	// done will be used to determine when a job
	// has been processed.
	done chan bool
}

type Worker struct {
	WorkerPool chan chan Job
	JobChannel chan Job
	exit       chan bool
}

// Constructor for a worker.
func NewWorker(workerPool chan chan Job) Worker {
	return Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		exit:       make(chan bool),
	}
}

// Constructor for a job.
func NewJob(request requestData, jobType JobType) (Job, chan bool) {
	switch jobType {
	case JOB_BEGIN_SESSION:
		done := make(chan bool)
		return JobBeginSession{request: request, done: done}, done
	case JOB_ADD_INTEGER:
		done := make(chan bool)
		return JobAddInteger{request: request, done: done}, done
	case JOB_END_SESSION:
		done := make(chan bool)
		return JobEndSession{request: request, done: done}, done
	}
	// Returning nil for now since we know this will never happen,
	// but ideally we should return an error message stating that
	// the given job type was not found.
	return nil, nil
}

// markDone methods will mark a job as "done"
// so that we don't terminate until the job has been finished.
// This is to ensure that the user receives any data that
// they are counting on, back (such as session ID or reservoir
// sample).

func (j JobBeginSession) markDone() {
	j.done <- true
}

func (j JobAddInteger) markDone() {
	j.done <- true
}

func (j JobEndSession) markDone() {
	j.done <- true
}

// Process will process a job and write to the HTTP
// response accordingly.

func (j JobBeginSession) Process() error {
	sessionID, err := api.BeginSession(j.request.Reservoir)
	if err != nil {
		http.Error(j.request.responseWriter, err.Error(),
			http.StatusInternalServerError)
		return err
	}

	jsonResponse, err := json.Marshal(struct{ SessionID uint64 }{sessionID})
	if err != nil {
		http.Error(j.request.responseWriter, err.Error(),
			http.StatusInternalServerError)
		return err
	}

	j.request.responseWriter.WriteHeader(http.StatusOK)
	j.request.responseWriter.Write(jsonResponse)
	return nil
}

func (j JobAddInteger) Process() error {
	// TODO: find a way to tell if they passed in a value or chose default 0.
	err := api.AddInteger(j.request.SessionID, j.request.IntegerToAdd)
	if err != nil {
		http.Error(j.request.responseWriter, err.Error(),
			http.StatusNotFound)
		return err
	}

	j.request.responseWriter.WriteHeader(http.StatusOK)
	return nil
}

func (j JobEndSession) Process() error {
	result, err := api.EndSession(j.request.SessionID)
	if err != nil {
		http.Error(j.request.responseWriter, err.Error(),
			http.StatusNotFound)
		return err
	}

	// Marshal response to JSON for the user.
	response := responseData{Sample: result}
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		http.Error(j.request.responseWriter, "Error converting response to JSON.",
			http.StatusInternalServerError)
		return err
	}

	j.request.responseWriter.WriteHeader(http.StatusOK)
	j.request.responseWriter.Write(jsonResponse)
	return nil
}

func (w Worker) Start() {
	go func() {
		for {
			// Add the worker to the worker queue.
			w.WorkerPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:
				// we have received a work request.
				if err := job.Process(); err != nil {
					// Ideally we should have some sort of logging system used
					// here. We should also keep a journal that maps job id's to
					// job data, that way we can go back and check the logs to
					// see what went wrong if anything abnormal happens. Here
					// we just print the job data and type.
					fmt.Printf("Error processing job: %v\n", err.Error())
				}
				job.markDone()
			case <-w.exit:
				// Stop upon receiving an exit signal.
				return
			}
		}
	}()
}

// Send an exit signal to the worker
// to stop running.
func (w Worker) Stop() {
	go func() {
		w.exit <- true
	}()
}
