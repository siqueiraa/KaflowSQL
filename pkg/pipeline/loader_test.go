package pipeline

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFromFile(t *testing.T) {
	pipelinePath := createTestPipeline(t)
	pipeline := loadAndValidatePipeline(t, pipelinePath)

	verifyBasicProperties(t, &pipeline)
	verifyOutputConfig(t, &pipeline)
	verifyEmissionRules(t, &pipeline)
	verifyStateConfig(t, &pipeline)
}

// createTestPipeline creates a test pipeline file and returns its path
func createTestPipeline(t *testing.T) string {
	tempDir := t.TempDir()
	pipelinePath := filepath.Join(tempDir, "test_pipeline.yaml")

	pipelineContent := getTestPipelineContent()
	err := os.WriteFile(pipelinePath, []byte(pipelineContent), 0600)
	if err != nil {
		t.Fatalf("Failed to write test pipeline: %v", err)
	}
	return pipelinePath
}

// getTestPipelineContent returns the YAML content for the test pipeline
func getTestPipelineContent() string {
	return `
name: test_pipeline
window: 1h

query: |
  SELECT 
    e.user_id,
    e.event_type,
    e.timestamp,
    u.name,
    u.email
  FROM events e
  LEFT JOIN users u ON e.user_id = u.user_id
  WHERE e.amount > 100

output:
  topic: enriched_events
  format: avro
  key: user_id

emission:
  type: smart
  require:
    - user_id
    - name
    - email

state:
  events:
    default_ttl: 1h
    overrides:
      events: 30m
      transactions: 2h
  dimensions:
    - users
    - products
`
}

// loadAndValidatePipeline loads a pipeline and validates it loaded successfully
func loadAndValidatePipeline(t *testing.T, pipelinePath string) Pipeline {
	pipeline, err := LoadFromFile(pipelinePath)
	if err != nil {
		t.Fatalf("Failed to load pipeline: %v", err)
	}
	return pipeline
}

// verifyBasicProperties validates basic pipeline properties
func verifyBasicProperties(t *testing.T, pipeline *Pipeline) {
	if pipeline.Name != "test_pipeline" {
		t.Errorf("Expected name 'test_pipeline', got '%s'", pipeline.Name)
	}
	if pipeline.Window != "1h" {
		t.Errorf("Expected window '1h', got '%s'", pipeline.Window)
	}
	if pipeline.Query == "" {
		t.Errorf("Query should not be empty")
	}
}

// verifyOutputConfig validates output configuration
func verifyOutputConfig(t *testing.T, pipeline *Pipeline) {
	if pipeline.Output.Topic != "enriched_events" {
		t.Errorf("Expected output topic 'enriched_events', got '%s'", pipeline.Output.Topic)
	}
	if pipeline.Output.Format != "avro" {
		t.Errorf("Expected output format 'avro', got '%s'", pipeline.Output.Format)
	}
	if pipeline.Output.Key != "user_id" {
		t.Errorf("Expected output key 'user_id', got '%s'", pipeline.Output.Key)
	}
}

// verifyEmissionRules validates emission rules configuration
func verifyEmissionRules(t *testing.T, pipeline *Pipeline) {
	if pipeline.Emission == nil {
		t.Fatalf("Emission rules should not be nil")
	}
	if pipeline.Emission.Type != "smart" {
		t.Errorf("Expected emission type 'smart', got '%s'", pipeline.Emission.Type)
	}
	expectedRequire := []string{"user_id", "name", "email"}
	if len(pipeline.Emission.Require) != len(expectedRequire) {
		t.Errorf("Expected %d required fields, got %d", len(expectedRequire), len(pipeline.Emission.Require))
	}
}

// verifyStateConfig validates state configuration
func verifyStateConfig(t *testing.T, pipeline *Pipeline) {
	if pipeline.State == nil {
		t.Fatalf("State configuration should not be nil")
	}
	if pipeline.State.Events.DefaultTTL != "1h" {
		t.Errorf("Expected default TTL '1h', got '%s'", pipeline.State.Events.DefaultTTL)
	}
	if pipeline.State.Events.Overrides["events"] != "30m" {
		t.Errorf("Expected events override '30m', got '%s'", pipeline.State.Events.Overrides["events"])
	}
	expectedDimensions := []string{"users", "products"}
	if len(pipeline.State.Dimensions) != len(expectedDimensions) {
		t.Errorf("Expected %d dimensions, got %d", len(expectedDimensions), len(pipeline.State.Dimensions))
	}
}

func TestLoadFromFileMinimal(t *testing.T) {
	// Test loading a minimal pipeline file
	tempDir := t.TempDir()
	pipelinePath := filepath.Join(tempDir, "minimal_pipeline.yaml")

	pipelineContent := `
name: minimal_pipeline
query: SELECT * FROM events
output:
  topic: output_events
  format: json
  key: id
`

	err := os.WriteFile(pipelinePath, []byte(pipelineContent), 0600)
	if err != nil {
		t.Fatalf("Failed to write minimal pipeline: %v", err)
	}

	pipeline, err := LoadFromFile(pipelinePath)
	if err != nil {
		t.Fatalf("Failed to load minimal pipeline: %v", err)
	}

	if pipeline.Name != "minimal_pipeline" {
		t.Errorf("Expected name 'minimal_pipeline', got '%s'", pipeline.Name)
	}

	if pipeline.Query != "SELECT * FROM events" {
		t.Errorf("Expected query 'SELECT * FROM events', got '%s'", pipeline.Query)
	}

	// Optional fields should be nil when not specified
	if pipeline.Emission != nil && len(pipeline.Emission.Require) > 0 {
		t.Errorf("Emission should be empty for minimal pipeline")
	}
}

func TestPipelineConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		pipeline    Pipeline
		expectValid bool
	}{
		{
			name: "valid pipeline",
			pipeline: Pipeline{
				Name:  "valid",
				Query: "SELECT * FROM events",
				Output: OutputConfig{
					Topic:  "output",
					Format: "json",
					Key:    "id",
				},
			},
			expectValid: true,
		},
		{
			name: "missing name",
			pipeline: Pipeline{
				Query: "SELECT * FROM events",
				Output: OutputConfig{
					Topic:  "output",
					Format: "json",
					Key:    "id",
				},
			},
			expectValid: false,
		},
		{
			name: "missing query",
			pipeline: Pipeline{
				Name: "missing_query",
				Output: OutputConfig{
					Topic:  "output",
					Format: "json",
					Key:    "id",
				},
			},
			expectValid: false,
		},
		{
			name: "missing output topic",
			pipeline: Pipeline{
				Name:  "missing_topic",
				Query: "SELECT * FROM events",
				Output: OutputConfig{
					Format: "json",
					Key:    "id",
				},
			},
			expectValid: false,
		},
		{
			name: "invalid format",
			pipeline: Pipeline{
				Name:  "invalid_format",
				Query: "SELECT * FROM events",
				Output: OutputConfig{
					Topic:  "output",
					Format: "invalid",
					Key:    "id",
				},
			},
			expectValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := validatePipelineConfig(&tt.pipeline)
			if valid != tt.expectValid {
				t.Errorf("Pipeline validation mismatch: got %v, want %v", valid, tt.expectValid)
			}
		})
	}
}

func TestStateConfigParsing(t *testing.T) {
	tempDir := t.TempDir()
	pipelinePath := filepath.Join(tempDir, "state_test.yaml")

	pipelineContent := `
name: state_test
query: SELECT * FROM events
output:
  topic: output
  format: json
  key: id

state:
  events:
    default_ttl: 2h
    overrides:
      high_priority: 4h
      low_priority: 30m
  dimensions:
    - static_data
    - reference_tables
`

	err := os.WriteFile(pipelinePath, []byte(pipelineContent), 0600)
	if err != nil {
		t.Fatalf("Failed to write state test pipeline: %v", err)
	}

	pipeline, err := LoadFromFile(pipelinePath)
	if err != nil {
		t.Fatalf("Failed to load state test pipeline: %v", err)
	}

	// Test state configuration parsing
	if pipeline.State.Events.DefaultTTL != "2h" {
		t.Errorf("Expected default TTL '2h', got '%s'", pipeline.State.Events.DefaultTTL)
	}

	if pipeline.State.Events.Overrides["high_priority"] != "4h" {
		t.Errorf("Expected high_priority override '4h', got '%s'", pipeline.State.Events.Overrides["high_priority"])
	}

	if pipeline.State.Events.Overrides["low_priority"] != "30m" {
		t.Errorf("Expected low_priority override '30m', got '%s'", pipeline.State.Events.Overrides["low_priority"])
	}

	expectedDimensions := []string{"static_data", "reference_tables"}
	for i, dim := range expectedDimensions {
		if i >= len(pipeline.State.Dimensions) || pipeline.State.Dimensions[i] != dim {
			t.Errorf("Expected dimension '%s' at index %d", dim, i)
		}
	}
}

func TestEmissionRulesParsing(t *testing.T) {
	tempDir := t.TempDir()
	pipelinePath := filepath.Join(tempDir, "emission_test.yaml")

	pipelineContent := `
name: emission_test
query: SELECT * FROM events
output:
  topic: output
  format: json
  key: id

emission:
  type: smart
  require:
    - user_id
    - session_id
    - event_type
    - timestamp
`

	err := os.WriteFile(pipelinePath, []byte(pipelineContent), 0600)
	if err != nil {
		t.Fatalf("Failed to write emission test pipeline: %v", err)
	}

	pipeline, err := LoadFromFile(pipelinePath)
	if err != nil {
		t.Fatalf("Failed to load emission test pipeline: %v", err)
	}

	if pipeline.Emission.Type != "smart" {
		t.Errorf("Expected emission type 'smart', got '%s'", pipeline.Emission.Type)
	}

	expectedFields := []string{"user_id", "session_id", "event_type", "timestamp"}
	if len(pipeline.Emission.Require) != len(expectedFields) {
		t.Errorf("Expected %d required fields, got %d", len(expectedFields), len(pipeline.Emission.Require))
	}

	for i, field := range expectedFields {
		if i >= len(pipeline.Emission.Require) || pipeline.Emission.Require[i] != field {
			t.Errorf("Expected required field '%s' at index %d", field, i)
		}
	}
}

func TestLoadFromFileErrors(t *testing.T) {
	t.Run("NonexistentFile", func(t *testing.T) {
		_, err := LoadFromFile("/nonexistent/pipeline.yaml")
		if err == nil {
			t.Errorf("Expected error for nonexistent file")
		}
	})

	t.Run("InvalidYAML", func(t *testing.T) {
		tempDir := t.TempDir()
		invalidPath := filepath.Join(tempDir, "invalid.yaml")

		invalidContent := `
name: invalid
query: SELECT * FROM events
output:
  topic: output
  invalid_yaml: [unclosed
`

		err := os.WriteFile(invalidPath, []byte(invalidContent), 0600)
		if err != nil {
			t.Fatalf("Failed to write invalid pipeline: %v", err)
		}

		_, err = LoadFromFile(invalidPath)
		if err == nil {
			t.Errorf("Expected error for invalid YAML")
		}
	})

	t.Run("EmptyFile", func(t *testing.T) {
		tempDir := t.TempDir()
		emptyPath := filepath.Join(tempDir, "empty.yaml")

		err := os.WriteFile(emptyPath, []byte(""), 0600)
		if err != nil {
			t.Fatalf("Failed to write empty pipeline: %v", err)
		}

		_, err = LoadFromFile(emptyPath)
		if err == nil {
			t.Errorf("Expected error for empty file")
		}
	})
}

// Helper function for validation (would be implemented in the actual code)
func validatePipelineConfig(p *Pipeline) bool {
	if p.Name == "" {
		return false
	}
	if p.Query == "" {
		return false
	}
	if p.Output.Topic == "" {
		return false
	}
	if p.Output.Format != "json" && p.Output.Format != "avro" {
		return false
	}
	return true
}
