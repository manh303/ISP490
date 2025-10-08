#!/bin/bash
"""
Docker Optimization Script
==========================
Giúp giảm dung lượng Docker từ 114GB xuống tối thiểu
"""

echo "🐳 Docker Optimization Starting..."
echo "=================================="

# Hiển thị dung lượng hiện tại
echo "📊 Current Docker disk usage:"
docker system df

echo ""
echo "🧹 Step 1: Cleaning up unused containers..."
docker container prune -f

echo ""
echo "🧹 Step 2: Removing unused images..."
docker image prune -a -f

echo ""
echo "🧹 Step 3: Cleaning up unused volumes..."
docker volume prune -f

echo ""
echo "🧹 Step 4: Removing build cache..."
docker builder prune -a -f

echo ""
echo "🧹 Step 5: Complete system cleanup..."
docker system prune -a --volumes -f

echo ""
echo "📊 After cleanup - Docker disk usage:"
docker system df

echo ""
echo "🎯 Optimization Tips for Future:"
echo "================================"
echo "1. Use multi-stage Docker builds"
echo "2. Optimize .dockerignore files"
echo "3. Use smaller base images (alpine)"
echo "4. Remove unnecessary packages after install"
echo "5. Combine RUN commands to reduce layers"
echo "6. Use specific version tags instead of 'latest'"

echo ""
echo "🚀 Recommended Actions:"
echo "======================"
echo "• Consider using alpine-based images"
echo "• Remove unused Spark workers if not needed"
echo "• Optimize data-pipeline image (6.41GB)"
echo "• Use .dockerignore to exclude large files"

echo ""
echo "✅ Docker optimization completed!"

# Show largest images for review
echo ""
echo "📈 Top 10 largest images:"
docker images --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}" | head -11

echo ""
echo "💡 To further optimize specific images:"
echo "   docker-compose build --no-cache <service-name>"
echo "   # With optimized Dockerfile"