package pipeline

import (
	"reflect"
	"testing"
)

func TestPipelineModelValidation(t *testing.T) {
	tests := []struct {
		name     string
		pipeline Pipeline
		valid    bool
	}{
		{
			name: "valid pipeline",
			pipeline: Pipeline{
				Name:  "test_pipeline",
				Query: "SELECT * FROM events",
				Output: OutputConfig{
					Topic:  "output_topic",
					Format: "json",
					Key:    "user_id",
				},
			},
			valid: true,
		},
		{
			name: "missing name",
			pipeline: Pipeline{
				Query: "SELECT * FROM events",
				Output: OutputConfig{
					Topic:  "output_topic",
					Format: "json",
				},
			},
			valid: false,
		},
		{
			name: "missing query",
			pipeline: Pipeline{
				Name: "test_pipeline",
				Output: OutputConfig{
					Topic:  "output_topic",
					Format: "json",
				},
			},
			valid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := validatePipeline(&tt.pipeline)
			if valid != tt.valid {
				t.Errorf("validatePipeline() = %v, want %v", valid, tt.valid)
			}
		})
	}
}

// Helper function for validation (would be implemented in the actual code)
func validatePipeline(p *Pipeline) bool {
	if p.Name == "" {
		return false
	}
	if p.Query == "" {
		return false
	}
	if p.Output.Topic == "" {
		return false
	}
	return true
}

func TestEmissionRules(t *testing.T) {
	emission := &EmissionRules{
		Type:    "smart",
		Require: []string{"user_id", "amount", "timestamp"},
	}

	if emission.Type != "smart" {
		t.Errorf("Expected emission type 'smart', got '%s'", emission.Type)
	}

	expectedFields := []string{"user_id", "amount", "timestamp"}
	if !reflect.DeepEqual(emission.Require, expectedFields) {
		t.Errorf("Expected required fields %v, got %v", expectedFields, emission.Require)
	}
}

func TestStateConfig(t *testing.T) {
	state := &StateConfig{
		Events: struct {
			DefaultTTL string            `yaml:"default_ttl"`
			Overrides  map[string]string `yaml:"overrides,omitempty"`
		}{
			DefaultTTL: "1h",
			Overrides: map[string]string{
				"transactions": "30m",
				"user_events":  "2h",
			},
		},
		Dimensions: []string{"user_profiles", "product_catalog"},
	}

	if state.Events.DefaultTTL != "1h" {
		t.Errorf("Expected default TTL '1h', got '%s'", state.Events.DefaultTTL)
	}

	if state.Events.Overrides["transactions"] != "30m" {
		t.Errorf("Expected transactions TTL '30m', got '%s'", state.Events.Overrides["transactions"])
	}

	expectedDimensions := []string{"user_profiles", "product_catalog"}
	if !reflect.DeepEqual(state.Dimensions, expectedDimensions) {
		t.Errorf("Expected dimensions %v, got %v", expectedDimensions, state.Dimensions)
	}
}
