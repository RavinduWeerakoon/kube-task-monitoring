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
)

type JobStore struct {
	jobs             map[string]*Job
	mu               sync.RWMutex
	opensearchClient *opensearchapi.Client
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
				Addresses: []string{"https://localhost:9200"},
				// Addresses: []string{"https://opensearch-service:9200"},
				Username: "admin", // For testing only. Don't store credentials in code.
				Password: "Hiran@0685N",
			},
		},
	)
	if err != nil {
		panic(fmt.Sprintf("cannot initialize OpenSearch client: %v", err))
	}
	ctx := context.Background()
	_, err = client.Indices.Exists(ctx, opensearchapi.IndicesExistsReq{Indices: []string{"jobs"}})
	client.Indices.Exists(ctx, opensearchapi.IndicesExistsReq{Indices: []string{"kube-events"}})

	if err != nil {
		_, err = client.Indices.Create(ctx, opensearchapi.IndicesCreateReq{Index: "jobs"})
		if err != nil {
			panic(fmt.Sprintf("cannot create index: %v", err))
		}
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
		s.SendJob(event)

		// if event.Reason == "SuccessfulCreate" {

		// 	involvedObjectId := event.InvolvedObject.UID
		// 	fmt.Println("involvedObjectId", involvedObjectId)
		// 	podId := getPodId(event.Message)

		// 	jobName := event.InvolvedObject.OwnerReferences[0].Name

		// 	fmt.Println("jobName is", jobName)

		// 	job, exists := s.GetJob(involvedObjectId)

		// 	attempt := Attempt{
		// 		ID:         podId,
		// 		StartTime:  event.Metadata.CreationTimestamp,
		// 		Duration:   0,
		// 		Status:     "Running",
		// 		Activities: []Activity{},
		// 		AttemptID:  podId,
		// 	}

		// 	if !exists {

		// 		job := &Job{
		// 			ID:         involvedObjectId,
		// 			JobName:    jobName,
		// 			Name:       event.InvolvedObject.Name,
		// 			Namespace:  event.InvolvedObject.Namespace,
		// 			StartTime:  event.Metadata.CreationTimestamp,
		// 			Status:     "Running",
		// 			Activities: []Attempt{},
		// 		}

		// 		job.Activities = append(job.Activities, attempt)
		// 		// s.jobs[involvedObjectId] = job
		// 		s.SaveJob(job)

		// 	} else {
		// 		//another pod is created if the job has failed
		// 		changeLastActivity(*job, "Failed", event)
		// 		job.Activities = append(job.Activities, attempt)
		// 		s.UpdateJob(job)

		// 	}

		// } else if event.Reason == "Completed" {
		// 	involvedObjectId := event.InvolvedObject.UID
		// 	job, exists := s.GetJob(involvedObjectId)
		// 	if !exists {
		// 		fmt.Println("Failed there's no job uid with that")

		// 	}
		// 	changeLastActivity(*job, "Success", event)
		// 	job.Status = "Success"
		// 	job.EndTime = event.Metadata.CreationTimestamp
		// 	job.Duration = event.Metadata.CreationTimestamp.Sub(job.StartTime).Seconds()
		// 	s.UpdateJob(job)
		// } else if event.Reason == "BackoffLimitExceeded" {
		// 	involvedObjectId := event.InvolvedObject.UID
		// 	job, exists := s.GetJob(involvedObjectId)
		// 	if !exists {
		// 		fmt.Println("Failed there's no job uid with that")

		// 	}

		// 	changeLastActivity(*job, "Failed", event)
		// 	job.Status = "Failed"
		// 	job.EndTime = event.Metadata.CreationTimestamp
		// 	job.Duration = event.Metadata.CreationTimestamp.Sub(job.StartTime).Seconds()
		// 	s.UpdateJob(job)
		// }

	}
}

// Replace in-memory storage with OpenSearch
func (s *JobStore) GetJob(id string) (*Job, bool) {
	ctx := context.Background()
	getResp, err := s.opensearchClient.Document.Get(
		ctx,
		opensearchapi.DocumentGetReq{
			Index:      "jobs",
			DocumentID: id,
		},
	)

	if err != nil {
		fmt.Printf("Error getting job: %v\n", err)
		return nil, false
	}

	var Response struct {
		Source Job `json:"_source"`
	}

	respAsJson, err := json.MarshalIndent(getResp, "", "  ")

	if err := json.Unmarshal(respAsJson, &Response); err != nil {
		fmt.Printf("Error unmarshaling response: %v\n", err)
		return nil, false
	}

	if err != nil {
		return nil, false
	}

	job := Response.Source

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

func (s *JobStore) SendJob(event K8sEvent) error {
	jobData, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("error marshaling the job to OpenSearch: %v", err)
	}
	ctx := context.Background()

	indexResp, err := s.opensearchClient.Index(
		ctx,
		opensearchapi.IndexReq{
			Index: "kube-events",
			Body:  strings.NewReader(string(jobData)),
		},
	)
	if err != nil {
		fmt.Println("failed to update document ", err)
		return nil
	}

	fmt.Printf("Document added to kube events : %s\n", indexResp.Result)
	return nil
}

func (s *JobStore) SaveJob(job *Job) error {
	ctx := context.Background()
	jobData, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("error marshaling the job to OpenSearch: %v", err)
	}
	docCreateResp, err := s.opensearchClient.Document.Create(
		ctx,
		opensearchapi.DocumentCreateReq{
			Index:      "jobs",
			DocumentID: job.ID,
			Body:       strings.NewReader(string(jobData)),
		},
	)
	if err != nil {
		fmt.Println("failed to insert document ", err)
	}
	fmt.Println("Inserting a document")
	fmt.Println("Document", docCreateResp.Result)

	return nil
}

func (s *JobStore) UpdateJob(job *Job) error {
	jobData, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("error marshaling the job to OpenSearch: %v", err)
	}
	ctx := context.Background()

	indexResp, err := s.opensearchClient.Index(
		ctx,
		opensearchapi.IndexReq{
			Index:      "jobs",
			DocumentID: job.ID,
			Body:       strings.NewReader(string(jobData)),
		},
	)
	if err != nil {
		fmt.Println("failed to update document ", err)
		return nil
	}

	fmt.Printf("Document: %s\n", indexResp.Result)
	return nil
}

// searchResp, err := client.Search(
// 	ctx,
// 	&opensearchapi.SearchReq{
// 		Indices: []string{"kube-events"},
// 		Params: opensearchapi.SearchParams{
// 			Query: `metadata.name: "simplejob"`,
// 			Sort:  []string{"metadata.creationTimestamp:asc"},
// 		},
// 	},
// )

func (s *JobStore) getJobs(name string) []Job {
	ctx := context.Background()
	searchResp, err := s.opensearchClient.Search(
		ctx,
		&opensearchapi.SearchReq{
			Indices: []string{"jobs"},
			Params: opensearchapi.SearchParams{
				Query: fmt.Sprintf(`jobName:%s`, name),
				Sort:  []string{"startTime:asc"},
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
				Source Job `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.NewDecoder(searchResp.Inspect().Response.Body).Decode(&result); err != nil {
		fmt.Printf("Error parsing search response: %v\n", err)
		return nil
	}

	jobs := make([]Job, 0, len(result.Hits.Hits))
	for _, hit := range result.Hits.Hits {
		jobs = append(jobs, hit.Source)
	}
	return jobs
}

func (s *JobStore) getJobsByEvents(events []K8sEvent) []Job {

	jobMap := make(map[string]*Job)

	for _, event := range events {
		if event.InvolvedObject.Kind == "Job" {
			involvedObjectId := event.InvolvedObject.UID
			jobName := event.InvolvedObject.OwnerReferences[0].Name

			job, exists := jobMap[involvedObjectId]

			attempt := Attempt{
				ID:         getPodId(event.Message),
				StartTime:  event.Metadata.CreationTimestamp,
				Duration:   0,
				Status:     "Running",
				Activities: []Activity{},
				AttemptID:  getPodId(event.Message),
			}

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
