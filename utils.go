package main

import (
	"fmt"
	"sync"
	"time"
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

func (s *JobStore) AddOrUpdateJob(event K8sEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if event.InvolvedObject.Kind == "Job" {
		if event.Reason == "SuccessfulCreate" {
			fmt.Printf("Job %s created\n", event.InvolvedObject.Name)
			jobID := event.InvolvedObject.UID
			job := &Job{
				ID:         jobID,
				Name:       event.InvolvedObject.Name,
				Namespace:  event.InvolvedObject.Namespace,
				StartTime:  event.Metadata.CreationTimestamp,
				Status:     "Running",
				Activities: []Activity{},
			}

			s.jobs[jobID] = job
		} else if event.Reason == "Completed" {
			jobID := event.InvolvedObject.UID
			job, exists := s.jobs[jobID]
			if exists {
				job.Status = "Completed"
				job.EndTime = time.Now()
				job.Duration = job.EndTime.Sub(job.StartTime).Seconds()
			}
		}
	} else if event.InvolvedObject.Kind == "Pod" {

		jobID := event.InvolvedObject.Labels.ControllerUID
		job, exists := s.jobs[jobID]

		if !exists {
			fmt.Printf("Job %s not found for event %s\n", jobID, event.Reason)
		}

		// Record activity
		activity := Activity{
			Name:      event.Reason,
			Message:   event.Message,
			StartTime: event.Metadata.CreationTimestamp,
			EndTime:   event.Metadata.CreationTimestamp,
		}
		activity.Duration = activity.EndTime.Sub(activity.StartTime).Seconds()
		job.Activities = append(job.Activities, activity)

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
