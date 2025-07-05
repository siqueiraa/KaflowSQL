# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2025-07-05

### 🎉 First Major Release!

**KaflowSQL v1.0.0** - A production-ready streaming ETL framework for real-time Kafka data joins.

### ✨ Core Features
- **SQL-Native Streaming Joins** - Write familiar SQL to join Kafka topics in real-time
- **High-Performance Processing** - 100K+ events/second with sub-millisecond latency (tested on Mac M3 MAX)
- **Intelligent State Management** - RocksDB persistence with configurable TTL and S3 checkpointing
- **Smart Emission Control** - Configurable emission strategies for data quality vs throughput
- **Multi-Platform Support** - Docker images for linux/amd64, linux/arm64

### 🏗️ Technical Architecture
- **DuckDB Integration** - In-memory analytics engine for SQL execution
- **Hash-Based Sharding** - 256-way concurrent processing for optimal performance  
- **Zero-Copy Processing** - Efficient memory usage with pointer-based row storage
- **Avro Schema Support** - Full Schema Registry integration with automatic schema evolution

### 📋 Configuration & Operations
- **YAML Pipeline Definition** - Simple, declarative pipeline configuration
- **Flexible State Management** - Event TTL with dimension table support
- **Docker-First Deployment** - Production-ready containerization
- **Comprehensive Documentation** - Complete configuration reference and best practices

### 🔧 Development & Quality
- **Comprehensive CI/CD** - GitHub Actions with automated testing, security scanning, and releases
- **Multi-Platform Builds** - Cross-platform binary distribution
- **Security Scanning** - Gosec and Trivy vulnerability detection
- **Professional Project Structure** - Open-source ready with proper documentation

### 🐛 Fixed
- Updated Docker registry configuration for Docker Hub distribution
- Enhanced pipeline examples with generic use cases

### 🚀 Performance & Compatibility
- **Cross-Platform Ready** - Builds and runs seamlessly on all supported platforms
- **Production Tested** - Optimized for high-throughput streaming workloads
- **Docker Optimized** - Multi-architecture container images for easy deployment

### 📦 Distribution
- **Docker Images**: `siqueiraa/kaflowsql:1.0.0` and `siqueiraa/kaflowsql:latest`
- **Binary Releases**: Cross-platform binaries for Linux, macOS, and Windows
- **Source Code**: Full source available on GitHub

---

## [Unreleased]

### Added
- Performance optimizations and monitoring improvements planned for next release

## [Previous Versions]

### Added
- Initial streaming ETL framework
- RocksDB state management
- DuckDB integration for SQL processing
- Kafka consumer/producer with Avro support
- Schema Registry integration
- TTL-based state cleanup
- S3 checkpointing for disaster recovery
- Pipeline configuration system
- Join processing with hash-based sharding

---
