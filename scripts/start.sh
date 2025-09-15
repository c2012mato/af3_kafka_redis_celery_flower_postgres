#!/bin/bash

# Streaming Pipeline Startup Script
# This script sets up and starts the entire streaming architecture

set -e

echo "🚀 Starting Streaming Pipeline Architecture..."

# Check if Docker and Docker Compose are installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Create necessary directories
echo "📁 Creating necessary directories..."
mkdir -p logs data/raw data/processed data/external

# Set proper permissions for Airflow
echo "🔐 Setting permissions for Airflow..."
sudo chown -R 50000:0 dags logs plugins scripts data
sudo chmod -R 755 dags logs plugins scripts data

# Build and start services
echo "🏗️ Building and starting services..."
docker-compose build
docker-compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 30

# Check service health
echo "🔍 Checking service health..."

services=("postgres" "redis" "kafka" "zookeeper" "airflow-webserver" "airflow-scheduler" "flower")

for service in "${services[@]}"; do
    if docker-compose ps | grep -q "${service}.*Up"; then
        echo "✅ $service is running"
    else
        echo "❌ $service is not running"
    fi
done

# Setup Kafka topics
echo "📡 Setting up Kafka topics..."
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic streaming_events --partitions 3 --replication-factor 1 --if-not-exists
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic user_activities --partitions 3 --replication-factor 1 --if-not-exists
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic system_metrics --partitions 3 --replication-factor 1 --if-not-exists

echo "🎉 Streaming Pipeline Architecture is ready!"
echo ""
echo "📊 Access the services:"
echo "   • Airflow UI: http://localhost:8080 (airflow/airflow)"
echo "   • Flower (Celery): http://localhost:5555"
echo "   • pgAdmin: http://localhost:5050 (admin@example.com/admin123)"
echo "   • Kafka UI: http://localhost:8090"
echo "   • PostgreSQL: localhost:5432 (airflow/airflow123)"
echo "   • Redis: localhost:6379 (password: redis123)"
echo ""
echo "🔧 Development services (use docker-compose.dev.yaml):"
echo "   • Jupyter: http://localhost:8888 (token: jupyter123)"
echo "   • Redis Commander: http://localhost:8081"
echo "   • Adminer: http://localhost:8082"
echo "   • Prometheus: http://localhost:9090"
echo "   • Grafana: http://localhost:3000 (admin/admin123)"
echo ""
echo "📝 To stop all services: docker-compose down"
echo "📝 To view logs: docker-compose logs -f [service_name]"