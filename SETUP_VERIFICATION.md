# Setup Verification Guide

This document provides step-by-step instructions to verify that your streaming pipeline architecture is working correctly.

## 🔍 Pre-Flight Checklist

Before starting the services, ensure:

1. **System Requirements**
   - Docker Engine 20.10+ installed
   - Docker Compose v2.0+ available
   - At least 4GB RAM available
   - 10GB+ disk space available

2. **Network Ports Available**
   ```bash
   # Check if ports are available
   netstat -tuln | grep -E ':5432|:5050|:5555|:6379|:8080|:8090|:9092|:2181'
   ```
   If any ports are in use, modify the docker-compose.yaml file accordingly.

## 🚀 Starting the Pipeline

1. **Navigate to project directory**
   ```bash
   cd af3_kafka_redis_celery_flower_postgres
   ```

2. **Start the pipeline**
   ```bash
   ./scripts/start.sh
   ```

3. **Wait for initialization** (approximately 2-3 minutes)
   - The script will show service status as they come online
   - Airflow initialization may take longer on first run

## ✅ Service Verification

### 1. Check Service Status
```bash
docker compose ps
```
All services should show "Up" status.

### 2. Verify Individual Services

#### PostgreSQL
```bash
# Test database connection
docker compose exec postgres psql -U airflow -d airflow -c "SELECT current_database(), current_user;"

# Check streaming schema
docker compose exec postgres psql -U airflow -d airflow -c "\dt streaming.*"
```

#### Redis
```bash
# Test Redis connection
docker compose exec redis redis-cli -a redis123 ping
# Should return: PONG

# Check Redis info
docker compose exec redis redis-cli -a redis123 info server
```

#### Kafka
```bash
# List Kafka topics
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Check topic details
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic streaming_events
```

#### Airflow
```bash
# Check Airflow version
docker compose exec airflow-webserver airflow version

# List DAGs
docker compose exec airflow-webserver airflow dags list

# Check DAG status
docker compose exec airflow-webserver airflow dags state streaming_pipeline_demo 2024-01-01
```

## 🌐 Web Interface Verification

### 1. Airflow Web UI
- **URL**: http://localhost:8080
- **Credentials**: airflow / airflow
- **What to check**:
  - Login successful
  - `streaming_pipeline_demo` DAG visible
  - No critical errors in logs

### 2. Flower (Celery Monitor)
- **URL**: http://localhost:5555
- **What to check**:
  - Celery workers visible
  - Task statistics showing
  - No offline workers

### 3. pgAdmin
- **URL**: http://localhost:5050
- **Credentials**: admin@example.com / admin123
- **What to check**:
  - Login successful
  - "Airflow PostgreSQL" server pre-configured
  - Can connect to database
  - `streaming` schema visible

### 4. Kafka UI
- **URL**: http://localhost:8090
- **What to check**:
  - Kafka cluster visible
  - Topics list showing created topics
  - Partition information displayed

## 🧪 Pipeline Testing

### 1. Manual DAG Trigger
```bash
# Trigger the streaming pipeline DAG
docker compose exec airflow-webserver airflow dags trigger streaming_pipeline_demo
```

### 2. Monitor DAG Execution
- Go to Airflow UI (http://localhost:8080)
- Click on `streaming_pipeline_demo` DAG
- Watch task progression in Graph view
- Check task logs for any errors

### 3. Verify Data Flow

#### Check Kafka Messages
```bash
# Produce a test message
docker compose exec kafka kafka-console-producer --bootstrap-server localhost:9092 --topic streaming_events
# Type a JSON message and press Enter
{"test": "message", "timestamp": "2024-01-01T12:00:00Z"}

# Consume messages
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic streaming_events --from-beginning
```

#### Check Redis Cache
```bash
# Check recent events in Redis
docker compose exec redis redis-cli -a redis123 lrange recent_events 0 5

# Check event cache
docker compose exec redis redis-cli -a redis123 keys "event:*"
```

#### Check PostgreSQL Data
```bash
# Connect to PostgreSQL and check data
docker compose exec postgres psql -U airflow -d airflow -c "
SELECT COUNT(*) as total_aggregations FROM streaming.event_aggregations;
SELECT event_type, COUNT(*) FROM streaming.event_aggregations GROUP BY event_type;
"
```

## 🔧 Development Environment Testing

### 1. Start Development Services
```bash
docker compose -f docker-compose.yaml -f docker-compose.dev.yaml up -d
```

### 2. Test Jupyter Notebook
- **URL**: http://localhost:8888
- **Token**: jupyter123
- Open `streaming_analysis.ipynb`
- Run all cells to test connectivity

### 3. Test Monitoring
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin123)
- **Redis Commander**: http://localhost:8081
- **Adminer**: http://localhost:8082

## 🐛 Troubleshooting

### Common Issues and Solutions

#### Services Not Starting
```bash
# Check Docker resources
docker system df
docker system prune  # If needed

# Check logs for specific service
docker compose logs -f [service-name]

# Restart specific service
docker compose restart [service-name]
```

#### Airflow Database Issues
```bash
# Reinitialize Airflow database
docker compose exec airflow-webserver airflow db reset --yes
docker compose exec airflow-webserver airflow db upgrade
docker compose exec airflow-webserver airflow users create \
  --username airflow \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password airflow
```

#### Kafka Connection Issues
```bash
# Check Kafka broker status
docker compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Recreate topics if needed
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic streaming_events
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic streaming_events --partitions 3 --replication-factor 1
```

#### Redis Connection Issues
```bash
# Test Redis without password
docker compose exec redis redis-cli ping

# Check Redis configuration
docker compose exec redis redis-cli config get "*"
```

### Log Analysis
```bash
# View all service logs
docker compose logs -f

# View specific timeframe
docker compose logs --since="2024-01-01T10:00:00" --until="2024-01-01T11:00:00"

# Follow logs for debugging
docker compose logs -f airflow-scheduler kafka postgres redis
```

## ✅ Success Criteria

Your streaming pipeline is successfully set up when:

1. ✅ All services show "Up" status in `docker compose ps`
2. ✅ Airflow UI loads and shows DAGs
3. ✅ Flower shows active Celery workers
4. ✅ pgAdmin connects to PostgreSQL successfully
5. ✅ Kafka topics are created and accessible
6. ✅ Redis responds to ping commands
7. ✅ Sample DAG runs successfully without errors
8. ✅ Data flows from Kafka → Redis → PostgreSQL
9. ✅ All web interfaces are accessible
10. ✅ No critical errors in service logs

## 🔄 Regular Maintenance

### Daily Checks
- Monitor disk usage: `docker system df`
- Check service logs for errors
- Verify DAG execution status

### Weekly Maintenance
- Clean old Docker images: `docker image prune`
- Review and clean old log files
- Check database size and performance

### Monthly Tasks
- Update Docker images to latest versions
- Review and tune configuration parameters
- Backup important data and configurations

## 📞 Getting Help

If you encounter issues:

1. Check the troubleshooting section above
2. Review service logs: `docker compose logs -f [service]`
3. Consult the official documentation for each component
4. Check GitHub issues for known problems
5. Create a new issue with detailed error logs

---

**Happy Streaming! 🎉**