# Contributing to KaflowSQL

Thank you for your interest in contributing to KaflowSQL! We welcome contributions from everyone.

## Getting Started

### Prerequisites

- Go 1.24.1 or later
- Docker and Docker Compose
- Git

### Development Setup

1. **Fork and Clone**
   ```bash
   git clone https://github.com/your-username/KaflowSQL.git
   cd KaflowSQL
   ```

2. **Set up Development Environment**
   ```bash
   make setup-dev
   ```

3. **Build and Test**
   ```bash
   make build
   make test
   ```

## Development Workflow

### Making Changes

1. **Create a Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make Your Changes**
   - Write your code
   - Add tests for new functionality
   - Update documentation as needed

3. **Test Your Changes**
   ```bash
   make check
   make test-coverage
   ```

4. **Commit Your Changes**
   ```bash
   git add .
   git commit -m "feat: add your feature description"
   ```

### Commit Message Convention

We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

- `feat:` new features
- `fix:` bug fixes
- `docs:` documentation changes
- `style:` formatting changes
- `refactor:` code refactoring
- `test:` adding or updating tests
- `chore:` maintenance tasks

### Code Style

- Run `make fmt` to format your code
- Run `make lint` to check for style issues
- Follow Go best practices and idioms
- Write clear, self-documenting code
- Add comments for complex logic

### Testing

- Write unit tests for new functionality
- Ensure all tests pass: `make test`
- Check test coverage: `make test-coverage`
- Run race detector: `make test-race`

## Submitting Changes

### Pull Request Process

1. **Push Your Branch**
   ```bash
   git push origin feature/your-feature-name
   ```

2. **Create Pull Request**
   - Use the provided PR template
   - Provide clear description of changes
   - Link related issues
   - Add screenshots/examples if applicable

3. **Code Review**
   - Address review feedback
   - Keep your branch up to date
   - Be responsive to comments

### Pull Request Guidelines

- Keep PRs focused and atomic
- Write clear PR titles and descriptions
- Include tests for new features
- Update documentation as needed
- Ensure CI checks pass

## Project Structure

```
├── cmd/                    # Application entry points
│   ├── engine/            # Main streaming engine
│   └── fakegen/           # Data generation tool
├── pkg/                   # Shared packages
│   ├── avro/             # Avro schema handling
│   ├── config/           # Configuration management
│   ├── duck/             # DuckDB integration
│   ├── engine/           # Core processing engine
│   ├── kafka/            # Kafka client wrappers
│   ├── pipeline/         # Pipeline definition
│   ├── schema/           # Schema management
│   ├── state/            # State management
│   └── ttlindex/         # TTL indexing
├── pipelines/            # Pipeline definitions
└── .github/              # GitHub workflows
```

## Architecture Overview

KaflowSQL is a streaming ETL framework with these key components:

- **Engine**: Processes events using stateful joins
- **State Management**: RocksDB for persistence
- **Schema Registry**: Avro schema management
- **DuckDB**: In-memory analytics engine
- **Pipeline System**: YAML-based configuration

## Running Tests

```bash
# Run all tests
make test

# Run with coverage
make test-coverage

# Run with race detector
make test-race

# Run benchmarks
make benchmark
```

## Docker Development

```bash
# Start full development environment
make docker-compose-dev

# Build Docker image
make docker-build

# Start KaflowSQL only
make docker-compose-up
```

## Documentation

- Update README.md for user-facing changes
- Update CLAUDE.md for development guidance
- Add godoc comments for public APIs
- Create examples for new features

## Getting Help

- Check existing [issues](https://github.com/siqueiraa/KaflowSQL/issues)
- Create a new issue for bugs or feature requests
- Join discussions in GitHub Discussions
- Read the [documentation](README.md)

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). Please read and follow it.

## License

By contributing to KaflowSQL, you agree that your contributions will be licensed under the Apache License 2.0.

---

Thank you for contributing to KaflowSQL! 🚀