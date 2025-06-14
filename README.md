# OHLCV Data Collector (Dockerized)

This is a containerized version of the OHLCV Data Collector, which collects and stores OHLCV (Open, High, Low, Close, Volume) data for various financial instruments.

## Prerequisites

- Docker
- Docker Compose

## Directory Structure

```
.
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .dockerignore
├── ohlc_data/          # Created automatically
├── logs/              # Created automatically
└── [Python scripts]
```

## Quick Start

1. Build and start the containers:
   ```bash
   docker-compose up -d
   ```

2. View logs:
   ```bash
   docker-compose logs -f
   ```

## Services

The application consists of four main services:

1. **ohlcv-collector**: Main data collection service
2. **monitor**: Monitors data collection status
3. **validator**: Validates collected data
4. **alert-check**: Checks for stale data and sends alerts

## Configuration

- Edit `config.py` to modify email settings and other configurations
- The data is stored in the `ohlc_data` directory
- Logs are stored in the `logs` directory

## Stopping the Services

```bash
docker-compose down
```

## Resetting the Collector

To reset the collector state:

1. Stop the services:
   ```bash
   docker-compose down
   ```

2. Run the reset script:
   ```bash
   docker-compose run --rm ohlcv-collector python reset_collector.py
   ```

3. Restart the services:
   ```bash
   docker-compose up -d
   ```

## Data Persistence

- OHLCV data is persisted in the `ohlc_data` directory
- Logs are persisted in the `logs` directory
- Both directories are mounted as volumes in the containers

## Troubleshooting

1. Check container logs:
   ```bash
   docker-compose logs -f [service-name]
   ```

2. Access container shell:
   ```bash
   docker-compose exec [service-name] /bin/bash
   ```

3. View container status:
   ```bash
   docker-compose ps
   ``` 