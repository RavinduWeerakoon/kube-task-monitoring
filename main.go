package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
)

var jobStore *JobStore

func main() {
	jobStore = NewJobStore()

	// Routes
	http.HandleFunc("/webhook", handleWebhook)
	http.HandleFunc("/jobs/", handleJobs)
	http.HandleFunc("/jobs", handleJobs)

	log.Printf("Server starting on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event K8sEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	jobStore.AddOrUpdateJob(event)
	w.WriteHeader(http.StatusOK)
}

func handleJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	// Check if requesting specific job
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")

	if len(parts) > 1 {
		jobID := parts[1]
		job, exists := jobStore.GetJob(jobID)
		if !exists {
			http.Error(w, "Job not found", http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(job)
		return
	}

	// Return all jobs
	jobs := jobStore.GetAllJobs()
	json.NewEncoder(w).Encode(jobs)
}
