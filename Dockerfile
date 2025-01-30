# Use the official Golang image to build the application
FROM golang:1.23.4-alpine AS builder

# Set the working directory
WORKDIR /app

# Copy the source code
COPY . .

# Build the application
RUN go build -o webhook-app .

# Use a minimal base image
FROM alpine:latest

# Install necessary libraries
RUN apk --no-cache add ca-certificates

# Copy the built application from the builder stage
COPY --from=builder /app/webhook-app /webhook-app

# Expose the port the application runs on
EXPOSE 8080

# Command to run the application
ENTRYPOINT ["/webhook-app"]