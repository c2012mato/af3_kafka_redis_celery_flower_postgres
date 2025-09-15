#!/bin/bash

# Cleanup script for the streaming pipeline

set -e

echo "🧹 Cleaning up Streaming Pipeline..."

# Stop all services
echo "🛑 Stopping all services..."
docker compose down

# Remove volumes (optional - uncomment if you want to remove all data)
# echo "🗑️ Removing volumes..."
# docker compose down -v

# Remove dangling images
echo "🔧 Removing dangling Docker images..."
docker image prune -f

# Clean up temporary files
echo "📁 Cleaning up temporary files..."
rm -rf logs/*.log
rm -rf data/temp/*

echo "✅ Cleanup completed!"
echo ""
echo "💡 To remove all data including volumes, run:"
echo "   docker compose down -v"
echo ""
echo "💡 To remove all Docker images, run:"
echo "   docker system prune -a"