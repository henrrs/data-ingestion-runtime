package minio

import (
	"context"
	"fmt"
	"io"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"landing-connector/internal/adapters/sink/pathing"
	"landing-connector/internal/config"
	"landing-connector/internal/model"
)

type Sink struct {
	cfg    config.Config
	client *minio.Client
}

func New(ctx context.Context, cfg config.Config) (*Sink, error) {
	options := &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinIO.AccessKey, cfg.MinIO.SecretKey, ""),
		Secure: cfg.MinIO.UseSSL,
		Region: "us-east-1",
	}
	if cfg.MinIO.ForcePathStyle {
		options.BucketLookup = minio.BucketLookupPath
	}

	client, err := minio.New(cfg.MinIO.Endpoint, options)
	if err != nil {
		return nil, fmt.Errorf("create minio client: %w", err)
	}

	exists, err := client.BucketExists(ctx, cfg.MinIO.Bucket)
	if err != nil {
		return nil, fmt.Errorf("check minio bucket: %w", err)
	}
	if !exists {
		if err := client.MakeBucket(ctx, cfg.MinIO.Bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("create minio bucket: %w", err)
		}
	}

	return &Sink{
		cfg:    cfg,
		client: client,
	}, nil
}

func (s *Sink) UploadWindowStream(ctx context.Context, window model.BatchWindow, reader io.Reader) (string, error) {
	if window.RecordCount == 0 {
		return "", nil
	}

	objectPath := pathing.BuildFilePath(s.cfg.MinIO.BasePath, pathing.FilePrefix(s.cfg), window, s.cfg.Output.FileExtension())
	partSize := uint64(s.cfg.MinIO.MultipartPartSizeMiB) * 1024 * 1024
	_, err := s.client.PutObject(ctx, s.cfg.MinIO.Bucket, objectPath, reader, -1, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
		PartSize:    partSize,
		NumThreads:  1,
	})
	if err != nil {
		return "", fmt.Errorf("put minio object %s: %w", objectPath, err)
	}

	return fmt.Sprintf("s3://%s/%s", s.cfg.MinIO.Bucket, objectPath), nil
}
