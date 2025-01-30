package main

import (
	"fmt"
	"strings"
	"sync"
)

type JobStore struct {
	jobs map[string]*Job
	mu   sync.RWMutex
}

func NewJobStore() *JobStore {
	return &JobStore{
		jobs: make(map[string]*Job),
	}
}

type FinalStatus struct {
	Job       string `json:"job"`
	Condition string `json:"condition"`
}

func getPodId(msg string) string {
	parts := strings.Fields(msg)
	return parts[2]

}

func changeLastActivity(job Job, newStatus string, event K8sEvent) {

	if len(job.Activities) > 0 {
		lastActivity := &job.Activities[len(job.Activities)-1]
		lastActivity.Status = newStatus
		lastActivity.EndTime = event.Metadata.CreationTimestamp
		lastActivity.Duration = event.Metadata.CreationTimestamp.Sub(lastActivity.StartTime).Seconds()

	}

}

func (s *JobStore) AddOrUpdateJob(event K8sEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if event.InvolvedObject.Kind == "Job" {

		if event.Reason == "SuccessfulCreate" {

			involvedObjectId := event.InvolvedObject.UID
			podId := getPodId(event.Message)

			job, exists := s.jobs[involvedObjectId]

			attempt := Attempt{
				ID:         podId,
				StartTime:  event.Metadata.CreationTimestamp,
				Duration:   0,
				Status:     "Running",
				Activities: []Activity{},
				AttemptID:  podId,
			}

			if !exists {

				job := &Job{
					ID:         involvedObjectId,
					Name:       event.InvolvedObject.Name,
					Namespace:  event.InvolvedObject.Namespace,
					StartTime:  event.Metadata.CreationTimestamp,
					Status:     "Running",
					Activities: []Attempt{},
				}

				job.Activities = append(job.Activities, attempt)
				s.jobs[involvedObjectId] = job

			} else {
				changeLastActivity(*job, "Failed", event)
				job.Activities = append(job.Activities, attempt)

			}

		} else if event.Reason == "Completed" {
			involvedObjectId := event.InvolvedObject.UID
			job, exists := s.jobs[involvedObjectId]
			if !exists {
				fmt.Println("Failed there's no job uid with that")

			}
			changeLastActivity(*job, "Success", event)
			job.Status = "Success"
			job.EndTime = event.Metadata.CreationTimestamp
			job.Duration = event.Metadata.CreationTimestamp.Sub(job.StartTime).Seconds()
		} else if event.Reason == "BackoffLimitExceeded" {
			involvedObjectId := event.InvolvedObject.UID
			job, exists := s.jobs[involvedObjectId]
			if !exists {
				fmt.Println("Failed there's no job uid with that")

			}

			changeLastActivity(*job, "Failed", event)
			job.Status = "Failed"
			job.EndTime = event.Metadata.CreationTimestamp
			job.Duration = event.Metadata.CreationTimestamp.Sub(job.StartTime).Seconds()
		}

	}
}

func (s *JobStore) GetJob(id string) (*Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, exists := s.jobs[id]
	return job, exists
}

func (s *JobStore) GetAllJobs() []Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	jobs := make([]Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		jobs = append(jobs, *job)
	}
	return jobs
}
