package webhook

import corev1 "k8s.io/api/core/v1"

func hasEnvVar(envVars []corev1.EnvVar, envVarKey string) bool {
	for _, e := range envVars {
		if e.Name == K8sEnvVarPrefix {
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
