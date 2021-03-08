package secretmanager

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	idlCore "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flytestdlib/logger"
	v1 "k8s.io/api/core/v1"
)

type FileEnvSecretManager struct {
	secretPath string
	envPrefix  string
}

func (f FileEnvSecretManager) Get(ctx context.Context, key string) (string, error) {
	envVar := fmt.Sprintf("%s%s", f.envPrefix, key)
	v, ok := os.LookupEnv(envVar)
	if ok {
		logger.Debugf(ctx, "Secret found %s", v)
		return v, nil
	}
	secretFile := filepath.Join(f.secretPath, key)
	if _, err := os.Stat(secretFile); err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("secrets not found - file [%s], Env [%s]", secretFile, envVar)
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

func (f FileEnvSecretManager) InjectK8s(ctx context.Context, key *idlCore.Secret, o v1.Pod) error {
	secret, err := f.Get(ctx, key.Name)
	if err != nil {
		return err
	}

	envVar := fmt.Sprintf("%s%s", f.envPrefix, key)
	for _, c := range append(o.Spec.Containers, o.Spec.InitContainers...) {
		c.Env = append(c.Env, v1.EnvVar{
			Name:  envVar,
			Value: secret,
		})
	}

	return nil
}

func NewFileEnvSecretManager(cfg *Config) core.SecretManager {
	return FileEnvSecretManager{
		secretPath: cfg.SecretFilePrefix,
		envPrefix:  cfg.EnvironmentPrefix,
	}
}
