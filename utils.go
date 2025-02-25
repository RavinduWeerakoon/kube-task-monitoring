package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	opensearch "github.com/opensearch-project/opensearch-go/v4"
	opensearchapi "github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/redis/go-redis/v9"
)

type JobStore struct {
	jobs             map[string]*Job
	mu               sync.RWMutex
	opensearchClient *opensearchapi.Client
	redisClient      *redis.Client
}

func NewJobStore() *JobStore {
	// Initialize the client with SSL/TLS enabled.

	// Initialize the client with SSL/TLS enabled.
	client, err := opensearchapi.NewClient(
		opensearchapi.Config{
			Client: opensearch.Config{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // For testing only. Use certificate for validation.
				},
				// Addresses: []string{"https://localhost:9200"},
				Addresses: []string{"https://localhost:9200"},
				Username:  "admin", // For testing only. Don't store credentials in code.
				Password:  "Hiran@0685N",
			},
		},
	)
	if err != nil {
		panic(fmt.Sprintf("cannot initialize OpenSearch client: %v", err))
	}

	rdb := redis.NewClient(&redis.Options{
		// Addr: "redis:6379",
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	return &JobStore{
		jobs:             make(map[string]*Job),
		opensearchClient: client,
		redisClient:      rdb,
	}
}

type FinalStatus struct {
	Job       string `json:"job"`
	Condition string `json:"condition"`
}

func getPodId(msg string) string {
	fmt.Println("Message: ", msg)
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

			podId := getPodId(event.Message)
			involvedObjectId := event.InvolvedObject.UID

			job, exists := s.GetJob(jobName, involvedObjectId)

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
					JobName:    jobName,
					Name:       event.InvolvedObject.Name,
					Namespace:  event.InvolvedObject.Namespace,
					StartTime:  event.Metadata.CreationTimestamp,
					Status:     "Running",
					Activities: []Attempt{},
				}

				job.Activities = append(job.Activities, attempt)
				s.SaveJob(job, jobName)

			} else {
				//another pod is created if the job has failed
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
			job, exists := s.GetJob(jobName, involvedObjectId)
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

// this function will be used to check the if the redis has the current job execution details
func (s *JobStore) GetJob(name string, involvedObjectId string) (*Job, bool) {
	ctx := context.Background()
	val, err := s.redisClient.Get(ctx, "job:"+name).Result()
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

	if job.ID != involvedObjectId {
		return nil, false
	}

	return &job, true
}

func (s *JobStore) GetAllJobs() []Job {
	ctx := context.Background()
	res, err := s.opensearchClient.Search(
		ctx,
		&opensearchapi.SearchReq{Indices: []string{"jobs"}},
	)
	if err != nil {
		fmt.Printf("Error retrieving jobs: %v\n", err)
		return nil
	}
	respAsJson, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		fmt.Printf("Error Converting jobs: %v\n", err)
	}

	var searchResult struct {
		Hits struct {
			Hits []struct {
				Source Job `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.NewDecoder(strings.NewReader(string(respAsJson))).Decode(&searchResult); err != nil {
		fmt.Printf("Error decoding search results: %v\n", err)
		return nil
	}

	jobs := make([]Job, 0, len(searchResult.Hits.Hits))
	for _, hit := range searchResult.Hits.Hits {
		jobs = append(jobs, hit.Source)
	}
	return jobs
}

func (s *JobStore) SaveJob(job *Job, name string) error {
	ctx := context.Background()
	jobData, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("error marshaling the job to redis: %v", err)
	}

	err = s.redisClient.Set(ctx, "job:"+name, jobData, 0).Err()
	if err != nil {
		return fmt.Errorf("error saving job to redis: %v", err)
	}
	return nil
}

func (s *JobStore) getJobsByEvents(events []K8sEvent) []Job {

	jobMap := make(map[string]*Job)

	for _, event := range events {
		if event.InvolvedObject.Kind == "Job" {
			involvedObjectId := event.InvolvedObject.UID
			jobName := event.InvolvedObject.OwnerReferences[0].Name

			job, exists := jobMap[involvedObjectId]

			if !exists {
				job = &Job{
					ID:         involvedObjectId,
					JobName:    jobName,
					Name:       event.InvolvedObject.Name,
					Namespace:  event.InvolvedObject.Namespace,
					StartTime:  event.Metadata.CreationTimestamp,
					Status:     "Running",
					Activities: []Attempt{},
				}
				jobMap[involvedObjectId] = job
			}

			if event.Reason == "SuccessfulCreate" {

				attempt := Attempt{
					ID:         getPodId(event.Message),
					StartTime:  event.Metadata.CreationTimestamp,
					Duration:   0,
					Status:     "Running",
					Activities: []Activity{},
					AttemptID:  getPodId(event.Message),
				}
				job.Activities = append(job.Activities, attempt)
			} else if event.Reason == "Completed" {
				changeLastActivity(*job, "Success", event)
				job.Status = "Success"
				job.EndTime = event.Metadata.CreationTimestamp
				job.Duration = event.Metadata.CreationTimestamp.Sub(job.StartTime).Seconds()
			} else if event.Reason == "BackoffLimitExceeded" {
				changeLastActivity(*job, "Failed", event)
				job.Status = "Failed"
				job.EndTime = event.Metadata.CreationTimestamp
				job.Duration = event.Metadata.CreationTimestamp.Sub(job.StartTime).Seconds()
			}
		}
	}

	jobs := make([]Job, 0, len(jobMap))
	for _, job := range jobMap {
		jobs = append(jobs, *job)
	}
	return jobs

}

func (s *JobStore) getEvents(jobName string) []K8sEvent {
	ctx := context.Background()
	// Execute the search request.
	searchResp, err := s.opensearchClient.Search(
		ctx,
		&opensearchapi.SearchReq{
			Indices: []string{"kube-events"},
			Params: opensearchapi.SearchParams{
				Query: fmt.Sprintf(`involvedObject.ownerReferences.name:%s`, jobName),
				Sort:  []string{"metadata.creationTimestamp:asc"},
			},
		},
	)

	if err != nil {
		fmt.Printf("Error executing search query: %v\n", err)
		return nil
	}

	var result struct {
		Hits struct {
			Hits []struct {
				Source K8sEvent `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.NewDecoder(searchResp.Inspect().Response.Body).Decode(&result); err != nil {
		fmt.Printf("Error parsing search response: %v\n", err)
		return nil
	}

	events := make([]K8sEvent, 0, len(result.Hits.Hits))
	for _, hit := range result.Hits.Hits {
		events = append(events, hit.Source)
	}
	return events
}

func (s *JobStore) GetJobs(jobName string) []Job {
	events := s.getEvents(jobName)
	return s.getJobsByEvents(events)
}
