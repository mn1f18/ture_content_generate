#!/bin/bash

# 设置颜色输出
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}开始部署 True Content 服务...${NC}"

# 检查是否安装了Docker和Docker Compose
if ! command -v docker &> /dev/null; then
    echo -e "${RED}错误: Docker未安装${NC}"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}错误: Docker Compose未安装${NC}"
    exit 1
fi

# 检查.env文件是否存在
if [ ! -f .env ]; then
    echo -e "${RED}错误: .env文件不存在，请创建并配置环境变量${NC}"
    exit 1
fi

# 创建日志文件（如果不存在）
touch app.log
chmod 666 app.log

echo -e "${YELLOW}日志文件已准备好...${NC}"

# 确保mcp-network网络存在
if ! docker network inspect mcp-network &> /dev/null; then
    echo -e "${YELLOW}创建mcp-network网络...${NC}"
    docker network create mcp-network
fi

# 构建和启动容器
echo -e "${GREEN}构建和启动True Content服务...${NC}"
docker-compose down || true
docker-compose build --no-cache
docker-compose up -d

# 检查容器是否正常启动
sleep 5
if [ "$(docker ps -q -f name=true-content-api)" ]; then
    echo -e "${GREEN}True Content服务已成功启动!${NC}"
    echo -e "${GREEN}API访问地址: http://localhost:5001${NC}"
    echo -e "${YELLOW}可以通过以下命令查看日志:${NC}"
    echo -e "${YELLOW}docker logs -f true-content-api${NC}"
else
    echo -e "${RED}启动失败，请检查日志:${NC}"
    echo -e "${YELLOW}docker logs true-content-api${NC}"
    exit 1
fi

echo -e "${GREEN}部署完成!${NC}" 