# Streaming Pipeline Architecture

A comprehensive streaming data pipeline using **Apache Airflow 3**, **Apache Kafka**, **Redis**, **PostgreSQL**, **pgAdmin**, **Celery**, and **Flower**.

## 🏗️ Architecture Overview

This project implements a modern streaming data architecture with the following components:

- **Apache Airflow 3.0.0** - Latest workflow orchestration and scheduling with enhanced API server
- **Apache Kafka** - Event streaming platform
- **Redis** - In-memory data store for caching and message brokering
- **PostgreSQL** - Primary database for data storage
- **pgAdmin** - PostgreSQL administration interface
- **Celery** - Distributed task queue
- **Flower** - Celery monitoring tool
- **Zookeeper** - Kafka cluster coordination
- **Kafka UI** - Kafka management interface

### New in Airflow 3.0.0 Upgrade:
- **API Server**: Dedicated service for backend API communication
- **SDK**: Modern DAG development with decorators and asset lineage
- **Enhanced Performance**: Optimized operators and execution patterns
- **Data Assets**: Built-in data lineage tracking

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose installed
- At least 4GB RAM available
- Ports 5432, 5050, 5555, 6379, 8080, 8090, 9092, 2181 available

### 1. Clone the Repository

```bash
git clone <repository-url>
cd af3_kafka_redis_celery_flower_postgres
```

### 2. Start the Pipeline

```bash
./scripts/start.sh
```

This script will:
- Create necessary directories
- Set proper permissions
- Build and start all services
- Setup Kafka topics
- Verify service health

### 3. Access the Services

| Service | URL | Credentials | Notes |
|---------|-----|-------------|-------|
| Airflow UI | http://localhost:8080 | airflow/airflow | Enhanced UI with Airflow 3.0.0 features |
| Airflow API Server | http://localhost:8081 | airflow/airflow | New dedicated API server for backend communication |
| Flower (Celery) | http://localhost:5555 | - | Celery worker monitoring |
| pgAdmin | http://localhost:5050 | admin@example.com/admin123 | PostgreSQL administration |
| Kafka UI | http://localhost:8090 | - | Kafka topic management |
| PostgreSQL | localhost:5432 | airflow/airflow123 | Database access |
| Redis | localhost:6379 | redis123 | Cache and message broker |

## 📊 Architecture Components

### Apache Airflow 3.0.0
- **Webserver**: Enhanced web UI for managing workflows with improved performance
- **API Server**: New dedicated API server for backend communication and external integrations
- **Scheduler**: Task scheduling and orchestration with optimized performance
- **Worker**: Celery worker for distributed task execution
- **Triggerer**: Handles deferrable operators with enhanced capabilities
- **Executor**: CeleryExecutor for distributed processing
- **SDK**: Modern SDK for DAG development with decorators and assets

### Apache Kafka
- **Broker**: Single-node Kafka cluster
- **Topics**: Pre-configured topics for different data streams
- **Zookeeper**: Cluster coordination
- **Kafka UI**: Web interface for topic management

### Redis
- **Caching**: Fast data access layer
- **Message Broker**: Celery task queue backend
- **Session Storage**: Temporary data storage

### PostgreSQL
- **Airflow Metadata**: DAG and task metadata
- **Streaming Schema**: Custom schema for streaming data
- **Data Warehouse**: Processed and aggregated data

## 🔄 Data Flow

1. **Data Ingestion**: Raw events are produced to Kafka topics
2. **Stream Processing**: Airflow DAGs consume and process Kafka messages
3. **Caching**: Processed data is cached in Redis for fast access
4. **Aggregation**: Data is aggregated and stored in PostgreSQL
5. **Monitoring**: Flower monitors Celery tasks, pgAdmin manages PostgreSQL

## 📁 Project Structure

```
.
├── dags/                          # Airflow DAGs
│   ├── streaming_pipeline_demo.py # Original streaming pipeline DAG (Airflow 3.0.0 compatible)
│   └── streaming_pipeline_sdk.py  # Modern SDK-based DAG with decorators and assets
├── plugins/                       # Airflow plugins
│   └── kafka_operators.py        # Custom Kafka operators (updated for 3.0.0)
├── scripts/                       # Utility scripts
│   ├── start.sh                  # Startup script
│   ├── cleanup.sh                # Cleanup script
│   ├── kafka_utils.py            # Kafka utilities
│   └── celery_config.py          # Celery configuration
├── init-scripts/                 # Database initialization
│   └── 01-init-streaming-schema.sql
├── pgadmin/                      # pgAdmin configuration
│   └── servers.json             # Pre-configured database connections
├── monitoring/                   # Monitoring configuration
│   ├── prometheus.yml           # Prometheus configuration
│   └── grafana/                 # Grafana dashboards and datasources
├── docker-compose.yaml          # Main services configuration (updated for Airflow 3.0.0)
├── docker-compose.dev.yaml      # Development services configuration
├── Dockerfile                   # Custom Airflow 3.0.0 image
├── requirements.txt             # Python dependencies (updated for 3.0.0)
├── airflow.cfg                  # Airflow configuration (enhanced for 3.0.0)
├── test_upgrade.py              # Upgrade validation tests
├── .env                         # Environment variables
└── README.md                    # This file
```
├── docker-compose.dev.yaml      # Development services
├── Dockerfile                   # Custom Airflow image
├── requirements.txt             # Python dependencies
├── airflow.cfg                  # Airflow configuration
├── .env                         # Environment variables
└── README.md                    # This file
```

## 🛠️ Development Setup

For development with additional tools:

```bash
# Start with development services
docker compose -f docker-compose.yaml -f docker-compose.dev.yaml up -d
```

Additional development services:
- **Jupyter**: http://localhost:8888 (token: jupyter123)
- **Redis Commander**: http://localhost:8081
- **Adminer**: http://localhost:8082
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin123)

## 📈 Sample Pipeline

The included `streaming_pipeline_demo.py` DAG demonstrates:

1. **Data Generation**: Creates sample streaming events
2. **Kafka Production**: Sends events to Kafka topics
3. **Stream Consumption**: Consumes events from Kafka
4. **Redis Caching**: Caches processed events in Redis
5. **Data Aggregation**: Aggregates data and stores in PostgreSQL
6. **Cleanup**: Removes old data to maintain performance

## 🔧 Configuration

### Environment Variables

Key environment variables in `.env`:

```bash
# PostgreSQL
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow123
POSTGRES_DB=airflow

# Redis
REDIS_HOST=redis
REDIS_PASSWORD=redis123

# Airflow
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CELERY__BROKER_URL=redis://:redis123@redis:6379/0
```

### Kafka Topics

Default topics created automatically:
- `streaming_events` - General event stream
- `user_activities` - User activity events
- `system_metrics` - System performance metrics
- `error_logs` - Error and exception logs
- `processed_events` - Processed and enriched events

## 📊 Monitoring and Observability

### Airflow Monitoring
- **Web UI**: Task status, logs, and DAG visualization
- **Flower**: Celery worker monitoring and task distribution
- **Health Checks**: Built-in health check endpoints

### Database Monitoring
- **pgAdmin**: Database administration and query interface
- **Custom Views**: Pre-built views for common analytics queries

### Infrastructure Monitoring
- **Prometheus**: Metrics collection (development setup)
- **Grafana**: Visualization dashboards (development setup)

## 🧪 Testing

### Test Kafka Integration

```bash
# Access Kafka container
docker compose exec kafka bash

# Create a test topic
kafka-topics --bootstrap-server localhost:9092 --create --topic test-topic --partitions 1 --replication-factor 1

# Produce test messages
kafka-console-producer --bootstrap-server localhost:9092 --topic test-topic

# Consume test messages
kafka-console-consumer --bootstrap-server localhost:9092 --topic test-topic --from-beginning
```

### Test Airflow DAGs

```bash
# List DAGs
docker compose exec airflow-webserver airflow dags list

# Test the original DAG
docker compose exec airflow-webserver airflow dags test streaming_pipeline_demo 2024-01-01

# Test the new SDK-based DAG
docker compose exec airflow-webserver airflow dags test streaming_pipeline_sdk 2024-01-01

# Test API server connectivity
curl -u airflow:airflow http://localhost:8081/health
```

### Airflow 3.0.0 Specific Features

```bash
# Test the API server
curl -u airflow:airflow http://localhost:8081/api/v1/dags

# Validate upgrade
python test_upgrade.py

# Check data assets and lineage (in Airflow UI)
# Navigate to Data -> Assets to see lineage tracking
```

## 🔒 Security Considerations

- **Default Passwords**: Change all default passwords in production
- **Network Security**: Configure proper firewall rules
- **SSL/TLS**: Enable encryption for production deployments
- **Authentication**: Configure proper authentication backends
- **Secrets Management**: Use external secret management systems

## 📝 Troubleshooting

### Common Issues

1. **Port Conflicts**: Ensure required ports are available
2. **Memory Issues**: Increase Docker memory allocation
3. **Permission Issues**: Check file permissions for Airflow directories
4. **Service Dependencies**: Wait for dependencies to be ready before starting dependent services

### Logs

```bash
# View all logs
docker compose logs -f

# View specific service logs
docker compose logs -f airflow-scheduler
docker compose logs -f kafka
docker compose logs -f postgres
```

### Health Checks

```bash
# Check service status
docker compose ps

# Check individual service health
docker compose exec postgres pg_isready -U airflow
docker compose exec redis redis-cli -a redis123 ping
```

## 🧹 Cleanup

### Stop Services

```bash
./scripts/cleanup.sh
```

### Remove All Data

```bash
# Remove all containers, networks, and volumes
docker compose down -v

# Remove all images
docker system prune -a
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

## 🔗 Useful Links

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Redis Documentation](https://redis.io/documentation)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Celery Documentation](https://docs.celeryproject.org/)

---

**Built with ❤️ for modern data streaming pipelines**
