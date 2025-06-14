#!/bin/bash

# Exit on error
set -e

# Pull latest changes
echo "Pulling latest changes..."
git pull

# Build and restart containers
echo "Building and restarting containers..."
docker-compose down
docker-compose build --no-cache
docker-compose up -d

# Show logs
echo "Container logs:"
docker-compose logs -f 