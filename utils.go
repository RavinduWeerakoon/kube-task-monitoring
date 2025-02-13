package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	opensearch "github.com/opensearch-project/opensearch-go"
	opensearchapi "github.com/opensearch-project/opensearch-go/opensearchapi"
)

type JobStore struct {
	jobs             map[string]*Job
	mu               sync.RWMutex
	opensearchClient *opensearch.Client
}

func NewJobStore() *JobStore {
	// Initialize the client with SSL/TLS enabled.
	client, err := opensearch.NewClient(opensearch.Config{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Addresses: []string{"https://localhost:9200"},
		Username:  "admin", // For testing only. Don't store credentials in code.
		Password:  "Hiran@0685N",
	})

	if err != nil {
		panic(fmt.Sprintf("cannot initialize OpenSearch client: %v", err))
	}

	return &JobStore{
		jobs:             make(map[string]*Job),
		opensearchClient: client,
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
			fmt.Println("involvedObjectId", involvedObjectId)
			podId := getPodId(event.Message)

			job, exists := s.GetJob(involvedObjectId)

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
				s.SaveJob(job)

			} else {
				//another pod is created if the job has failed
				changeLastActivity(*job, "Failed", event)
				job.Activities = append(job.Activities, attempt)
				s.UpdateJob(job)

			}

		} else if event.Reason == "Completed" {
			involvedObjectId := event.InvolvedObject.UID
			job, exists := s.GetJob(involvedObjectId)
			if !exists {
				fmt.Println("Failed there's no job uid with that")

			}
			changeLastActivity(*job, "Success", event)
			job.Status = "Success"
			job.EndTime = event.Metadata.CreationTimestamp
			job.Duration = event.Metadata.CreationTimestamp.Sub(job.StartTime).Seconds()
			s.UpdateJob(job)
		} else if event.Reason == "BackoffLimitExceeded" {
			involvedObjectId := event.InvolvedObject.UID
			job, exists := s.GetJob(involvedObjectId)
			if !exists {
				fmt.Println("Failed there's no job uid with that")

			}

			changeLastActivity(*job, "Failed", event)
			job.Status = "Failed"
			job.EndTime = event.Metadata.CreationTimestamp
			job.Duration = event.Metadata.CreationTimestamp.Sub(job.StartTime).Seconds()
			s.UpdateJob(job)
		}

	}
}

// Replace in-memory storage with OpenSearch
func (s *JobStore) GetJob(id string) (*Job, bool) {
	ctx := context.Background()
	req := opensearchapi.GetRequest{
		Index:      "jobs",
		DocumentID: id,
	}

	res, err := req.Do(ctx, s.opensearchClient)

	if err != nil {
		fmt.Printf("Error getting job: %v\n", err)
		return nil, false
	}
	defer res.Body.Close()

	// fmt.Println("res", res.StatusCode)
	// fmt.Println("res", res)

	if res.StatusCode == http.StatusNotFound {
		return nil, false
	}

	var response struct {
		Source Job `json:"_source"`
	}
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		fmt.Printf("Error decoding job: %v\n", err)
		return nil, false
	}
	job := response.Source

	return &job, true

}

func (s *JobStore) GetAllJobs() []Job {
	ctx := context.Background()
	res, err := s.opensearchClient.Search(
		s.opensearchClient.Search.WithContext(ctx),
		s.opensearchClient.Search.WithIndex("jobs"),
		s.opensearchClient.Search.WithSize(1000), // Adjust size as needed
	)
	if err != nil {
		fmt.Printf("Error retrieving jobs: %v\n", err)
		return nil
	}
	defer res.Body.Close()

	var searchResult struct {
		Hits struct {
			Hits []struct {
				Source Job `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.NewDecoder(res.Body).Decode(&searchResult); err != nil {
		fmt.Printf("Error decoding search results: %v\n", err)
		return nil
	}

	jobs := make([]Job, 0, len(searchResult.Hits.Hits))
	for _, hit := range searchResult.Hits.Hits {
		jobs = append(jobs, hit.Source)
	}
	return jobs
}

func (s *JobStore) SaveJob(job *Job) error {
	ctx := context.Background()
	jobData, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("error marshaling the job to OpenSearch: %v", err)
	}
	req := opensearchapi.IndexRequest{
		Index:      "jobs",
		DocumentID: job.ID,
		Body:       strings.NewReader(string(jobData)),
	}
	insertResponse, err := req.Do(ctx, s.opensearchClient)
	if err != nil {
		fmt.Println("failed to insert document ", err)
	}
	fmt.Println("Inserting a document")
	fmt.Println(insertResponse)
	defer insertResponse.Body.Close()

	return nil
}

func (s *JobStore) UpdateJob(job *Job) error {
	jobData, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("error marshaling the job to OpenSearch: %v", err)
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("/jobs/_update/%s", job.ID), bytes.NewReader(jobData))
	if err != nil {
		return fmt.Errorf("failed to create update request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.opensearchClient.Transport.Perform(req)
	if err != nil {
		return fmt.Errorf("failed to perform update request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to update document, status: %s", resp.Status)
	}
	return nil
}
