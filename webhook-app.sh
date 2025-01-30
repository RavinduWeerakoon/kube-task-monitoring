#!/bin/bash

#create the docker container
docker build -t webhook-app . -f Dockerfile

# Delete the existing deployment
kubectl delete deployment webhook-app

# Apply the new deployment configuration
kubectl apply -f webhook-app-deployment.yaml