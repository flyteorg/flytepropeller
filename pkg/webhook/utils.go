package webhook

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"

	corev1 "k8s.io/api/core/v1"
)

func hasEnvVar(envVars []corev1.EnvVar, envVarKey string) bool {
	for _, e := range envVars {
		if e.Name == envVarKey {
			return true
		}
	}

	return false
}

func UpdateVolumeMounts(containers []corev1.Container, secretName string) []corev1.Container {
	res := make([]corev1.Container, 0, len(containers))
	for _, c := range containers {
		c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{
			Name:      secretName,
			ReadOnly:  true,
			MountPath: K8sSecretPathPrefix + secretName,
		})

		res = append(res, c)
	}

	return res
}

func UpdateEnvVars(containers []corev1.Container, envVar corev1.EnvVar) []corev1.Container {
	res := make([]corev1.Container, 0, len(containers))
	for _, c := range containers {
		if !hasEnvVar(c.Env, envVar.Name) {
			c.Env = append(c.Env, envVar)
		}

		res = append(res, c)
	}

	return res
}

// ReadFile reads file contents given a path
func ReadFile(filepath string) (*bytes.Buffer, error) {
	f, err := os.Open(filepath)
	if err != nil {
		if f != nil {
			err2 := f.Close()
			if err2 != nil {
				return nil, fmt.Errorf("failed to create and close file. Create Error: %w. Close Error: %v", err, err2)
			}
		}

		return nil, fmt.Errorf("failed to create file. Error: %w", err)
	}

	buf, err := ioutil.ReadAll(f)
	if err != nil {
		err2 := f.Close()
		if err2 != nil {
			return nil, fmt.Errorf("failed to write and close file. Write Error: %w. Close Error: %v", err, err2)
		}

		return nil, err
	}

	return bytes.NewBuffer(buf), nil
}
