package uiapi

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"

	bbolt "go.etcd.io/bbolt"
)

var hfBucketName = []byte("hf_metadata")
var customModelsBucketName = []byte("custom_models_v2")

type hfMetadataStore struct {
	db *bbolt.DB
}

func newHFMetadataStore(path string) (*hfMetadataStore, error) {
	if path == "" {
		return nil, errors.New("metadata db path is empty")
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	db, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, err
	}

	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(hfBucketName)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(customModelsBucketName)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &hfMetadataStore{db: db}, nil
}

func (s *hfMetadataStore) Get(key string) (hfMetadata, bool, error) {
	var meta hfMetadata
	var ok bool

	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(hfBucketName)
		if bucket == nil {
			return nil
		}

		raw := bucket.Get([]byte(key))
		if raw == nil {
			return nil
		}

		if err := json.Unmarshal(raw, &meta); err != nil {
			return err
		}

		ok = true
		return nil
	})

	return meta, ok, err
}

func (s *hfMetadataStore) Set(key string, meta hfMetadata) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(hfBucketName)
		if bucket == nil {
			return errors.New("metadata bucket missing")
		}
		return bucket.Put([]byte(key), data)
	})
}

func (s *hfMetadataStore) ListCustomModels() ([]customModelEntry, error) {
	models := make([]customModelEntry, 0)

	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(customModelsBucketName)
		if bucket == nil {
			return nil
		}

		return bucket.ForEach(func(key []byte, value []byte) error {
			var entry customModelEntry
			if err := json.Unmarshal(value, &entry); err != nil {
				if err := json.Unmarshal(key, &entry); err != nil {
					legacy := strings.TrimSpace(string(key))
					if legacy == "" {
						return nil
					}
					href, label := modelLinkForRef(legacy)
					models = append(models, customModelEntry{Model: legacy, DisplayName: simplifyModelDisplayName(legacy), LinkHref: href, LinkLabel: label})
					return nil
				}
			}
			models = append(models, entry)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(models, func(i, j int) bool { return models[i].DisplayName < models[j].DisplayName })
	return models, nil
}

func (s *hfMetadataStore) AddCustomModel(entry customModelEntry) error {
	if strings.TrimSpace(entry.Model) == "" {
		return errors.New("custom model ref is empty")
	}
	if strings.TrimSpace(entry.DisplayName) == "" {
		entry.DisplayName = simplifyModelDisplayName(entry.Model)
	}
	if strings.TrimSpace(entry.LinkHref) == "" || strings.TrimSpace(entry.LinkLabel) == "" {
		entry.LinkHref, entry.LinkLabel = modelLinkForRef(entry.Model)
	}
	if strings.TrimSpace(entry.Source) == "" {
		entry.Source = modelSourceLabel(entry.Model)
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(customModelsBucketName)
		if bucket == nil {
			return errors.New("custom models bucket missing")
		}
		return bucket.Put([]byte(entry.Model), data)
	})
}
