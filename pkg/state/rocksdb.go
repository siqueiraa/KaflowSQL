package state

// -----------------------------------------------------------------------------
// Standard libraries
// -----------------------------------------------------------------------------
import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	// -------------------------------------------------------------------------
	// External dependencies
	// -------------------------------------------------------------------------
	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dgraph-io/badger/v4"

	"github.com/siqueiraa/KaflowSQL/pkg/config"
)

const (
	dirMode           = 0o755       // Default directory permissions
	maxFileSize       = 100 << 20   // 100MB limit per file to prevent zip bombs
	topicOffsetBase   = 10          // Base for offset string parsing
	offsetBitSize     = 64          // Bit size for offset parsing
	dsStoreFile       = ".DS_Store" // macOS metadata file name
	appleDoublePrefix = "._"        // AppleDouble resource fork prefix
	topicSplitParts   = 2           // Number of parts when splitting topic:key
)

/* -------------------------------------------------------------------------- */
/*  Data structures                                                           */
/* -------------------------------------------------------------------------- */

// RocksDBState keeps the original name for full compatibility, but the internal
// engine is now BadgerDB (pure Go, no CGO).
type RocksDBState struct {
	db         *badger.DB
	ttlDefault time.Duration
	ttlByTopic map[string]time.Duration
	basePath   string
	name       string
	cfg        config.AppConfig
}

// storedValue remains to preserve disk-format compatibility for pre-existing
// data that uses a JSON wrapper to record the timestamp.
type storedValue struct {
	Timestamp int64  `json:"ts"`
	Payload   []byte `json:"payload"`
}

func NewRocksDBState(pipelineName string, cfg *config.AppConfig, ttlByTopic map[string]time.Duration) (*RocksDBState, error) {
	baseDir := filepath.Dir(cfg.State.RocksDB.Path)
	dbDir := filepath.Base(cfg.State.RocksDB.Path)
	path := filepath.Join(baseDir, pipelineName, dbDir)

	if err := os.MkdirAll(path, dirMode); err != nil {
		return nil, fmt.Errorf("failed to create state path: %w", err)
	}

	st := &RocksDBState{
		ttlDefault: cfg.State.TTL,
		ttlByTopic: ttlByTopic,
		basePath:   path,
		name:       pipelineName,
		cfg:        *cfg,
	}

	// Check if the directory is empty to decide on checkpoint restoration
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read RocksDB path: %w", err)
	}

	if len(entries) == 0 {
		if restoreErr := st.RestoreCheckpointIfAvailable(); restoreErr != nil {
			return nil, fmt.Errorf("failed to restore checkpoint: %w", restoreErr)
		}
		log.Printf("[State] Checkpoint restored for pipeline %s", pipelineName)
	} else {
		log.Printf("[State] Skipping checkpoint restore for %s: directory is not empty", pipelineName)
	}

	opts := badger.DefaultOptions(path).
		WithLoggingLevel(badger.ERROR)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	st.db = db
	return st, nil
}

/* -------------------------------------------------------------------------- */
/*  CRUD methods — public API unchanged                                       */
/* -------------------------------------------------------------------------- */

// Put stores a key/value pair together with its timestamp, identical signature.
func (r *RocksDBState) Put(topic, key string, value []byte, ts time.Time) error {
	combinedKey := fmt.Appendf(nil, "%s:%s", topic, key)
	v := storedValue{Timestamp: ts.Unix(), Payload: value}
	data, _ := json.Marshal(v)

	return r.db.Update(func(txn *badger.Txn) error {
		return txn.Set(combinedKey, data)
	})
}

// ForEach itera sobre todas as chaves que começam com prefix
// chamando fn(keySemPrefix, payload, ts) para cada uma delas.
func (r *RocksDBState) ForEach(prefix string,
	fn func(key string, value []byte, ts time.Time) error) error {
	return r.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		p := []byte(prefix)
		for it.Seek(p); it.ValidForPrefix(p); it.Next() {
			item := it.Item()

			var sv storedValue
			if err := item.Value(func(v []byte) error {
				return json.Unmarshal(v, &sv)
			}); err != nil {
				return err
			}

			key := string(item.Key())[len(prefix):] // remove prefix
			if err := fn(key, sv.Payload, time.Unix(sv.Timestamp, 0)); err != nil {
				return err
			}
		}
		return nil
	})
}

// Get retrieves a key/value and enforces TTL logic exactly as before.
func (r *RocksDBState) Get(topic, key string) ([]byte, error) {
	combinedKey := fmt.Appendf(nil, "%s:%s", topic, key)

	var data []byte
	err := r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(combinedKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return errors.New("not found")
		}
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			data = append([]byte(nil), v...)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	var stored storedValue
	if err := json.Unmarshal(data, &stored); err != nil {
		return nil, err
	}

	ttl := r.ttlDefault
	if t, ok := r.ttlByTopic[topic]; ok {
		ttl = t
	}
	if ttl > 0 && time.Since(time.Unix(stored.Timestamp, 0)) > ttl {
		go func() {
			if err := r.DeleteAsync(combinedKey); err != nil {
				log.Printf("Error deleting expired key %s: %v", combinedKey, err)
			}
		}()
		return nil, errors.New("expired")
	}
	return stored.Payload, nil
}

// StatsByTopic counts keys per topic using an iterator.
func (r *RocksDBState) StatsByTopic() (map[string]int, error) {
	stats := make(map[string]int)
	err := r.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			key := string(it.Item().Key())
			topic := strings.SplitN(key, ":", topicSplitParts)[0]
			stats[topic]++
		}
		return nil
	})
	return stats, err
}

// CreateCheckpointIfEnabled makes a local copy of the database directory and
// optionally uploads it to S3, preserving the original tar-file name.
func (r *RocksDBState) CreateCheckpointIfEnabled() error {
	if !r.cfg.State.RocksDB.Checkpoint.Enabled {
		return nil
	}

	cpPath := filepath.Join(r.basePath, "checkpoint")
	_ = os.RemoveAll(cpPath)
	if err := copyDir(r.basePath, cpPath); err != nil {
		return err
	}

	if r.cfg.State.RocksDB.Checkpoint.S3.Enabled {
		return r.uploadToS3(cpPath)
	}
	return nil
}

// uploadToS3 compresses a checkpoint directory and uploads it; signature,
// variable names and log messages stay exactly the same.
func (r *RocksDBState) uploadToS3(cpPath string) error {
	s3cfg := r.cfg.State.RocksDB.Checkpoint.S3
	ctx := context.Background()

	awsCfg, err := awsConfig.LoadDefaultConfig(ctx,
		awsConfig.WithRegion(s3cfg.Region),
		awsConfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(s3cfg.AccessKey, s3cfg.SecretKey, ""),
		),
	)
	if err != nil {
		return err
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(s3cfg.Endpoint)
		o.UsePathStyle = true
	})
	uploader := manager.NewUploader(client)

	tarFile := fmt.Sprintf("/tmp/%s-checkpoint.tar.gz", r.name)
	if tarErr := tarGz(cpPath, tarFile); tarErr != nil {
		return tarErr
	}

	file, err := os.Open(tarFile)
	if err != nil {
		return err
	}
	defer file.Close()

	key := fmt.Sprintf("%s%s.tar.gz", s3cfg.Prefix, r.name)
	res, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s3cfg.Bucket),
		Key:    aws.String(key),
		Body:   file,
	})
	if err != nil {
		return err
	}
	log.Printf("[Checkpoint] Uploaded to %s", res.Location)
	return nil
}

// RestoreCheckpointIfAvailable downloads and extracts a checkpoint from S3 if
// one is present. Public API unchanged.
func (r *RocksDBState) RestoreCheckpointIfAvailable() error {
	s3cfg := r.cfg.State.RocksDB.Checkpoint.S3
	if !r.cfg.State.RocksDB.Checkpoint.Enabled || !s3cfg.Enabled {
		return nil
	}

	ctx := context.Background()
	awsCfg, err := awsConfig.LoadDefaultConfig(ctx,
		awsConfig.WithRegion(s3cfg.Region),
		awsConfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(s3cfg.AccessKey, s3cfg.SecretKey, ""),
		),
	)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(s3cfg.Endpoint)
		o.UsePathStyle = true
	})

	key := fmt.Sprintf("%s%s.tar.gz", s3cfg.Prefix, r.name)
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s3cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		log.Printf("[Checkpoint] No checkpoint found in S3: %v", err)
		return nil
	}
	defer resp.Body.Close()

	log.Printf("[Checkpoint] Restoring checkpoint for %s from S3…", r.name)
	return untarGz(resp.Body, r.basePath)
}

/* -------------------------------------------------------------------------- */
/*  Helpers (signatures unmodified)                                           */
/* -------------------------------------------------------------------------- */

// DeleteAsync deletes a key in a background goroutine.
func (r *RocksDBState) DeleteAsync(key []byte) error {
	return r.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// SaveOffset and GetOffset keep identical behavior and signatures.
func (r *RocksDBState) SaveOffset(topic string, partition int, offset int64) error {
	key := fmt.Sprintf("%s-%d-offset", topic, partition)
	val := []byte(fmt.Sprintf("%d", offset))
	return r.Put(topic, key, val, time.Now())
}

func (r *RocksDBState) GetOffset(topic string, partition int) (int64, error) {
	key := fmt.Sprintf("%s-%d-offset", topic, partition)
	raw, err := r.Get(topic, key)
	if err != nil {
		return 0, fmt.Errorf("failed to get offset for %s/%d: %w", topic, partition, err)
	}
	return strconv.ParseInt(string(raw), topicOffsetBase, offsetBitSize)
}

/* -------------------------------------------------------------------------- */
/*  tar/untar utilities (unchanged code)                                      */
/* -------------------------------------------------------------------------- */

func tarGz(source, target string) error {
	out, err := os.Create(target)
	if err != nil {
		return err
	}
	defer out.Close()

	cmd := exec.Command("tar", "-czf", "-", "-C", source, ".")
	cmd.Stdout = out
	return cmd.Run()
}

// copyDir recursively copies the directory tree from src to dst,
// skipping any files or directories whose names start with "._"
// or are exactly ".DS_Store".
func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		name := info.Name()
		if strings.HasPrefix(name, appleDoublePrefix) || name == dsStoreFile {
			// Skip AppleDouble resource forks and Finder metadata files
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)

		if info.IsDir() {
			// Create directory with the same permissions
			return os.MkdirAll(target, info.Mode())
		}

		// Copy file contents
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()

		out, err := os.Create(target)
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			out.Close()
			return err
		}
		return out.Close()
	})
}

// untarGz extracts a gzip-compressed tar archive from reader into the
// target directory, skipping entries whose base names start with "._"
// or are exactly ".DS_Store".
func untarGz(reader io.Reader, target string) error {
	gzr, err := gzip.NewReader(reader)
	if err != nil {
		return err
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)
	return processArchiveEntries(tr, target)
}

// processArchiveEntries processes all entries in the tar archive
func processArchiveEntries(tr *tar.Reader, target string) error {
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if shouldSkipEntry(hdr.Name) {
			continue
		}

		if err := validateAndProcessEntry(hdr, tr, target); err != nil {
			return err
		}
	}
	return nil
}

// shouldSkipEntry checks if an archive entry should be skipped
func shouldSkipEntry(name string) bool {
	base := filepath.Base(name)
	return strings.HasPrefix(base, appleDoublePrefix) || base == dsStoreFile
}

// validateAndProcessEntry validates security constraints and processes the entry
func validateAndProcessEntry(hdr *tar.Header, tr *tar.Reader, target string) error {
	if err := validateEntrySize(hdr); err != nil {
		return err
	}

	path, err := validateAndCleanPath(hdr.Name, target)
	if err != nil {
		return err
	}

	return extractEntry(hdr, tr, path)
}

// validateEntrySize checks if the entry size is within limits
func validateEntrySize(hdr *tar.Header) error {
	if hdr.Size > maxFileSize {
		return fmt.Errorf("file %s too large: %d bytes (max %d)", hdr.Name, hdr.Size, maxFileSize)
	}
	return nil
}

// validateAndCleanPath validates and cleans the extraction path
func validateAndCleanPath(name, target string) (string, error) {
	cleanedName := filepath.Clean(name)
	if strings.Contains(cleanedName, "..") || strings.HasPrefix(cleanedName, "/") {
		return "", fmt.Errorf("invalid file path: %s (path traversal detected)", name)
	}
	return filepath.Join(target, cleanedName), nil
}

// extractEntry extracts a single archive entry to the filesystem
func extractEntry(hdr *tar.Header, tr *tar.Reader, path string) error {
	switch hdr.Typeflag {
	case tar.TypeDir:
		return os.MkdirAll(path, dirMode)
	case tar.TypeReg:
		return extractRegularFile(tr, path)
	default:
		return nil
	}
}

// extractRegularFile extracts a regular file from the archive
func extractRegularFile(tr *tar.Reader, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), dirMode); err != nil {
		return err
	}

	out, err := os.Create(path)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, io.LimitReader(tr, maxFileSize))
	return err
}
