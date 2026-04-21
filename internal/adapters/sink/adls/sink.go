package adls

import (
	"context"
	"fmt"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/filesystem"

	"landing-connector/internal/adapters/sink/pathing"
	"landing-connector/internal/config"
	"landing-connector/internal/model"
)

type Sink struct {
	cfg      config.Config
	fsClient *filesystem.Client
}

func New(_ context.Context, cfg config.Config) (*Sink, error) {
	cred, err := buildCredential(cfg.ADLS.Credential)
	if err != nil {
		return nil, err
	}

	filesystemURL := fmt.Sprintf("https://%s.dfs.core.windows.net/%s", cfg.ADLS.AccountName, cfg.ADLS.Filesystem)
	fsClient, err := filesystem.NewClient(filesystemURL, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("create adls filesystem client: %w", err)
	}

	return &Sink{
		cfg:      cfg,
		fsClient: fsClient,
	}, nil
}

func (s *Sink) UploadWindowFile(ctx context.Context, window model.BatchWindow, sourceFile *os.File, _ int64) (string, error) {
	if len(window.Records) == 0 {
		return "", nil
	}

	filePath := pathing.BuildFilePath(s.cfg.ADLS.BasePath, pathing.FilePrefix(s.cfg), window, s.cfg.Output.FileExtension())
	fileClient := s.fsClient.NewFileClient(filePath)
	if _, err := fileClient.Create(ctx, nil); err != nil {
		return "", fmt.Errorf("create adls file %s: %w", filePath, err)
	}

	if _, err := sourceFile.Seek(0, 0); err != nil {
		return "", fmt.Errorf("rewind temp file for adls upload: %w", err)
	}

	if err := uploadFile(ctx, fileClient, sourceFile); err != nil {
		return "", fmt.Errorf("upload adls file %s: %w", filePath, err)
	}

	return filePath, nil
}

func buildCredential(spec config.CredentialSpec) (azcore.TokenCredential, error) {
	switch spec.Mode {
	case "", "default_azure_credential":
		return azidentity.NewDefaultAzureCredential(nil)
	case "client_secret":
		return azidentity.NewClientSecretCredential(spec.TenantID, spec.ClientID, spec.ClientSecret, nil)
	default:
		return nil, fmt.Errorf("unsupported adls credential mode: %s", spec.Mode)
	}
}

func uploadFile(ctx context.Context, fileClient *file.Client, source *os.File) error {
	return fileClient.UploadFile(ctx, source, nil)
}
