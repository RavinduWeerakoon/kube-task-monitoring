## Rebuilding the Docker image


```bash
docker build -t your-docker-repo/webhook-app:latest .
docker push your-docker-repo/webhook-app:latest```

Redeploy the application
```bash
kubectl apply -f webhook-app-deployment.yaml
kubectl apply -f webhook-app-service.yaml```