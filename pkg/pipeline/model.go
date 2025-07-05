package pipeline

type Pipeline struct {
	Name     string         `yaml:"name"`
	Window   string         `yaml:"window"`
	Query    string         `yaml:"query"`
	Output   OutputConfig   `yaml:"output"`
	Emission *EmissionRules `yaml:"emission,omitempty"`
	State    *StateConfig   `yaml:"state,omitempty"`
}

type OutputConfig struct {
	Topic  string            `yaml:"topic"`
	Format string            `yaml:"format"`
	Schema map[string]string `yaml:"schema,omitempty"`
	Key    string            `yaml:"key,omitempty"`
}

type EmissionRules struct {
	Type    string   `yaml:"type"`
	Require []string `yaml:"require"`
}

// StateConfig now reflects the new YAML layout:
//
//	state:
//	  events:
//	    default_ttl: 1h
//	    overrides:
//	      transactions: 30m
//	  dimensions:
//	    - user_profiles
type StateConfig struct {
	// Fast‐changing topics: default TTL + per‑topic overrides
	Events struct {
		DefaultTTL string            `yaml:"default_ttl"`         // e.g. "1h"
		Overrides  map[string]string `yaml:"overrides,omitempty"` // e.g. {"transactions":"30m"}
	} `yaml:"events"`

	// Slow‐changing lookup topics that never expire
	Dimensions []string `yaml:"dimensions,omitempty"`
}
