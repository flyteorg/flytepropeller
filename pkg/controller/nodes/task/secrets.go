package task

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/lyft/flytestdlib/logger"
)

type fileEnvSecretManager struct {
	secretPath string
}

func (f fileEnvSecretManager) Get(ctx context.Context, key string) (string, error) {
	v, ok := os.LookupEnv(key)
	if ok {
		return v, nil
	}
	secretFile := filepath.Join(f.secretPath, key)
	if _, err := os.Stat(secretFile); err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("secrets not found")
		}
		return "", err
	}
	logger.Debugf(ctx, "reading secrets from filePath [%s]", secretFile)
	b, err := ioutil.ReadFile(secretFile)
	if err != nil {
		return "", err
	}
	return string(b), err
}
