package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
)

type JobStore struct {
	jobs        map[string]*Job
	mu          sync.RWMutex
	redisClient *redis.Client
}

func NewJobStore() *JobStore {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		// Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	return &JobStore{
		jobs:        make(map[string]*Job),
		redisClient: rdb,
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

		jobName := event.InvolvedObject.OwnerReferences[0].Name

		if event.Reason == "SuccessfulCreate" {

			involvedObjectId := event.InvolvedObject.UID
			podId := getPodId(event.Message)

			job, exists := s.GetJob(involvedObjectId, jobName)

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
				// s.jobs[involvedObjectId] = job
				s.SaveJob(job, jobName)

			} else {
				changeLastActivity(*job, "Failed", event)
				job.Activities = append(job.Activities, attempt)
				s.SaveJob(job, jobName)

			}

		} else if event.Reason == "Completed" {
			involvedObjectId := event.InvolvedObject.UID
			job, exists := s.GetJob(involvedObjectId, jobName)
			if !exists {
				fmt.Println("Failed there's no job uid with that")

			}
			changeLastActivity(*job, "Success", event)
			job.Status = "Success"
			job.EndTime = event.Metadata.CreationTimestamp
			job.Duration = event.Metadata.CreationTimestamp.Sub(job.StartTime).Seconds()
			s.SaveJob(job, jobName)
		} else if event.Reason == "BackoffLimitExceeded" {
			involvedObjectId := event.InvolvedObject.UID
			job, exists := s.GetJob(involvedObjectId, jobName)
			if !exists {
				fmt.Println("Failed there's no job uid with that")

			}

			changeLastActivity(*job, "Failed", event)
			job.Status = "Failed"
			job.EndTime = event.Metadata.CreationTimestamp
			job.Duration = event.Metadata.CreationTimestamp.Sub(job.StartTime).Seconds()
			s.SaveJob(job, jobName)
		}

	}
}

// Replace in-memory storage with Redis
func (s *JobStore) GetJob(id string, name string) (*Job, bool) {
	ctx := context.Background()
	val, err := s.redisClient.Get(ctx, "job:"+name+":"+id).Result()
	if err == redis.Nil {
		return nil, false
	} else if err != nil {
		fmt.Printf("Error retrieving job: %v\n", err)
		return nil, false
	}

	var job Job
	if err := json.Unmarshal([]byte(val), &job); err != nil {
		fmt.Printf("Error unmarshaling job: %v\n", err)
		return nil, false
	}
	return &job, true
}

func (s *JobStore) GetAllJobs(name string) []Job {
	ctx := context.Background()
	keys, err := s.redisClient.Keys(ctx, "job:"+name+":*").Result()
	if err != nil {
		fmt.Printf("Error retrieving job keys: %v\n", err)
		return nil
	}

	jobs := make([]Job, 0, len(keys))
	for _, key := range keys {
		val, err := s.redisClient.Get(ctx, key).Result()
		if err == nil {
			var job Job
			if json.Unmarshal([]byte(val), &job) == nil {
				jobs = append(jobs, job)
			}
		}
	}
	return jobs
}

func (s *JobStore) SaveJob(job *Job, name string) error {
	ctx := context.Background()
	jobData, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("error marshaling the job to redis: %v", err)
	}

	err = s.redisClient.Set(ctx, "job:"+name+":"+job.ID, jobData, 0).Err()
	if err != nil {
		return fmt.Errorf("error saving job to redis: %v", err)
	}
	return nil
}
