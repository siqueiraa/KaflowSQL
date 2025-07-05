package config

import (
	"log"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Named type to allow reuse and clearer code
type KafkaConfig struct {
	Brokers        []string `yaml:"brokers"`
	SchemaRegistry string   `yaml:"schemaRegistry"`
	UseAvro        bool     `yaml:"useAvro"`
}

type AppConfig struct {
	Kafka KafkaConfig `yaml:"kafka"`

	Emitter struct {
		Interval time.Duration `yaml:"interval"`
	} `yaml:"emitter"`

	State struct {
		TTL time.Duration `yaml:"ttl"` // used if the pipeline has no `window`

		RocksDB struct {
			Path       string `yaml:"path"`
			Checkpoint struct {
				Enabled  bool          `yaml:"enabled"`
				Interval time.Duration `yaml:"interval"`

				S3 struct {
					Enabled   bool   `yaml:"enabled"`
					Bucket    string `yaml:"bucket"`
					Region    string `yaml:"region"`
					AccessKey string `yaml:"accessKey"`
					SecretKey string `yaml:"secretKey"`
					Endpoint  string `yaml:"endpoint"`
					Prefix    string `yaml:"prefix"`
				} `yaml:"s3"`
			} `yaml:"checkpoint"`
		} `yaml:"rocksdb"`
	} `yaml:"state"`

	Engine struct {
		DefaultWindow      string        `yaml:"defaultWindow"`
		DuckDBMemory       string        `yaml:"duckdbMemory"`
		CheckpointInterval time.Duration `yaml:"checkpointInterval"`
	}
}

// Load reads and parses a YAML config file into an AppConfig struct.
// It will terminate the program if the file is not found or invalid.
func Load(path string) AppConfig {
	// Initialize with defaults
	cfg := AppConfig{
		Emitter: struct {
			Interval time.Duration `yaml:"interval"`
		}{
			Interval: 1 * time.Second, // Default emitter interval
		},
		State: struct {
			TTL     time.Duration `yaml:"ttl"`
			RocksDB struct {
				Path       string `yaml:"path"`
				Checkpoint struct {
					Enabled  bool          `yaml:"enabled"`
					Interval time.Duration `yaml:"interval"`
					S3       struct {
						Enabled   bool   `yaml:"enabled"`
						Bucket    string `yaml:"bucket"`
						Region    string `yaml:"region"`
						AccessKey string `yaml:"accessKey"`
						SecretKey string `yaml:"secretKey"`
						Endpoint  string `yaml:"endpoint"`
						Prefix    string `yaml:"prefix"`
					} `yaml:"s3"`
				} `yaml:"checkpoint"`
			} `yaml:"rocksdb"`
		}{
			TTL: 1 * time.Hour, // Default state TTL
		},
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		log.Fatalf("Config file not found: %s", path)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("Error reading config file: %v", err)
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("Error parsing config file: %v", err)
	}

	return cfg
}
