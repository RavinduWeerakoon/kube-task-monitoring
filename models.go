package main

import "time"

type Activity struct {
	Name      string    `json:"name"`
	Message   string    `json:"message"`
	StartTime time.Time `json:"startTime"`
	EndTime   time.Time `json:"endTime"`
	Duration  float64   `json:"duration"`
}

type Job struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	Namespace  string     `json:"namespace"`
	StartTime  time.Time  `json:"startTime"`
	EndTime    time.Time  `json:"endTime"`
	Duration   float64    `json:"duration"`
	Status     string     `json:"status"`
	Activities []Activity `json:"activities"`
}

type K8sEvent struct {
	Metadata struct {
		Name              string    `json:"name"`
		Namespace         string    `json:"namespace"`
		UID               string    `json:"uid"`
		ResourceVersion   string    `json:"resourceVersion"`
		CreationTimestamp time.Time `json:"creationTimestamp"`
	} `json:"metadata"`
	Reason         string `json:"reason"`
	Message        string `json:"message"`
	Type           string `json:"type"`
	InvolvedObject struct {
		Kind      string `json:"kind"`
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
		UID       string `json:"uid"`
		Labels    struct {
			BatchControllerUID string `json:"batch.kubernetes.io/controller-uid"`
			BatchJobName       string `json:"batch.kubernetes.io/job-name"`
			ControllerUID      string `json:"controller-uid"`
			JobName            string `json:"job-name"`
		}
	} `json:"involvedObject"`
}
