package main

import (
	"encoding/json"
	"log"
	"net/http"
)

var jobStore *JobStore

func enableCORS(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Add CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		// Handle preflight requests
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		handler(w, r)
	}
}

func main() {
	jobStore = NewJobStore()

	// Routes
	http.HandleFunc("/webhook", enableCORS(handleWebhook))
	http.HandleFunc("/jobs/", enableCORS(handleJobs))
	http.HandleFunc("/jobs", enableCORS(handleJobs))

	log.Printf("Server starting on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func handleWebhook(w http.ResponseWriter, r *http.Request) {

	// body, err := io.ReadAll(r.Body)
	// if err != nil {
	// 	http.Error(w, "Cannot read body", http.StatusBadRequest)
	// 	return
	// }
	// fmt.Println(string(body))

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

	// Get jobName from query parameters
	jobName := r.URL.Query().Get("jobName")

	// Return all jobs
	jobs := jobStore.GetAllJobs(jobName)

	json.NewEncoder(w).Encode(jobs)
}
