#!/bin/bash

# KaflowSQL Test Execution Script
# This script runs comprehensive tests for the KaflowSQL project

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to run a command with status reporting
run_with_status() {
    local cmd="$1"
    local description="$2"
    
    print_status "Running: $description"
    
    if eval "$cmd"; then
        print_success "$description completed successfully"
        return 0
    else
        print_error "$description failed"
        return 1
    fi
}

# Main test execution
main() {
    print_status "Starting KaflowSQL comprehensive test suite..."
    echo
    
    # Check if we're in the right directory
    if [[ ! -f "go.mod" ]] || [[ ! -f "Makefile" ]]; then
        print_error "This script must be run from the KaflowSQL root directory"
        exit 1
    fi
    
    # Set test mode
    TEST_MODE="${1:-unit}"
    
    case "$TEST_MODE" in
        "unit")
            print_status "Running unit tests only"
            run_unit_tests
            ;;
        "integration")
            print_status "Running integration tests"
            run_integration_tests
            ;;
        "all")
            print_status "Running all tests"
            run_all_tests
            ;;
        "coverage")
            print_status "Running tests with coverage analysis"
            run_coverage_tests
            ;;
        "performance")
            print_status "Running performance tests"
            run_performance_tests
            ;;
        *)
            print_error "Invalid test mode: $TEST_MODE"
            print_status "Usage: $0 [unit|integration|all|coverage|performance]"
            exit 1
            ;;
    esac
    
    print_success "All tests completed successfully!"
}

run_unit_tests() {
    print_status "=== Unit Tests ==="
    
    # Download dependencies
    run_with_status "make deps" "Downloading dependencies"
    
    # Format check
    run_with_status "make fmt" "Checking code formatting"
    
    # Linting
    if command -v golangci-lint &> /dev/null; then
        run_with_status "make lint" "Running linter"
    else
        print_warning "golangci-lint not found, skipping lint check"
    fi
    
    # Vet
    run_with_status "make vet" "Running go vet"
    
    # Unit tests
    run_with_status "make test" "Running unit tests"
    
    # Race condition tests
    run_with_status "make test-race" "Running race condition tests"
    
    # Benchmarks
    run_with_status "make test-bench" "Running benchmarks"
}

run_integration_tests() {
    print_status "=== Integration Tests ==="
    
    # Check if Kafka is available for integration tests
    if [[ -z "${KAFKA_BROKER}" ]]; then
        print_warning "KAFKA_BROKER environment variable not set"
        print_warning "Integration tests may be skipped"
    fi
    
    # Run integration tests
    run_with_status "make test-integration" "Running integration tests"
}

run_all_tests() {
    run_unit_tests
    echo
    run_integration_tests
}

run_coverage_tests() {
    print_status "=== Coverage Analysis ==="
    
    # Run tests with coverage
    run_with_status "make test-coverage-full" "Generating coverage report"
    
    # Display coverage summary
    if [[ -f "coverage.out" ]]; then
        print_status "Coverage Summary:"
        go tool cover -func=coverage.out | tail -1
        
        if [[ -f "coverage.html" ]]; then
            print_success "Coverage report generated: coverage.html"
        fi
    fi
}

run_performance_tests() {
    print_status "=== Performance Tests ==="
    
    # Run extended benchmarks
    run_with_status "make test-performance" "Running performance benchmarks"
    
    print_status "Performance test results saved"
}

# Cleanup function
cleanup() {
    print_status "Cleaning up test artifacts..."
    
    # Remove temporary test files
    find . -name "*.test" -delete 2>/dev/null || true
    find . -name "*.prof" -delete 2>/dev/null || true
    
    # Clean up test databases
    rm -rf /tmp/kaflowsql_test 2>/dev/null || true
}

# Set up signal handlers
trap cleanup EXIT
trap 'print_error "Test execution interrupted"; exit 1' INT TERM

# Run main function
main "$@"