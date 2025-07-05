# KaflowSQL Examples

This directory contains example configurations and pipelines for KaflowSQL.

## Directory Structure

```
examples/
├── pipelines/          # Pipeline configuration examples
├── configs/           # Configuration file examples
└── README.md         # This file
```

## Pipeline Examples

### `user_activity.yaml`
A comprehensive example showing user event enrichment with profile data and transaction history.

### `transaction.yaml`
A complex financial transaction processing pipeline with extensive business logic and transformations.

## Configuration Examples

### `config.yaml.example`
Basic configuration template with placeholder values for local development.

### `config.docker.yaml`
Configuration optimized for Docker container deployment.

### `config.production.yaml`
Production-ready configuration with security and performance settings.

## Usage

1. **Copy examples to active directory:**
   ```bash
   cp examples/pipelines/user_activity.yaml pipelines/
   cp examples/configs/config.yaml.example config.yaml
   ```

2. **Customize for your environment:**
   - Update Kafka broker addresses
   - Configure Schema Registry URL
   - Set appropriate TTL values
   - Adjust S3 settings for checkpointing

3. **Test your configuration:**
   ```bash
   make check
   make build
   ./bin/engine
   ```

## Important Notes

- **Production Data**: The examples contain references to production systems. Always customize for your environment.
- **Security**: Never commit real credentials. Use environment variables or secure secret management.
- **Performance**: Adjust TTL and buffer settings based on your throughput requirements.

For more information, see the main [README.md](../README.md) documentation.