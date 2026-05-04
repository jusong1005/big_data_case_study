#!/usr/bin/env bash
set -euo pipefail

source /etc/profile.d/bigdata-modern.sh

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="/home/hadoop/export/workspace/user_profile_visualization_build"

export MYSQL_HOST="${MYSQL_HOST:-localhost}"
export MYSQL_PORT="${MYSQL_PORT:-3306}"
export MYSQL_DATABASE="${MYSQL_DATABASE:-big_data_case_study}"
export MYSQL_USERNAME="${MYSQL_USERNAME:-bigdata}"
export MYSQL_PASSWORD="${MYSQL_PASSWORD:-BigData@123456}"
export REDIS_HOST="${REDIS_HOST:-localhost}"
export REDIS_PORT="${REDIS_PORT:-6379}"

rm -rf "$BUILD_DIR"
mkdir -p "$(dirname "$BUILD_DIR")"
mkdir -p "$BUILD_DIR"
cp "$PROJECT_DIR/pom.xml" "$BUILD_DIR/pom.xml"
cp -R "$PROJECT_DIR/src" "$BUILD_DIR/src"
cd "$BUILD_DIR"

mvn -q -DskipTests package

echo "Starting user profile visualization backend: http://localhost:8001/user_profile"
java -jar target/user_profile_visualization-0.0.1-SNAPSHOT.jar
