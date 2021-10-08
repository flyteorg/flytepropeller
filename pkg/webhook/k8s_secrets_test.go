package webhook

import (
	"context"
	"testing"

	"github.com/go-test/deep"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	coreIdl "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	corev1 "k8s.io/api/core/v1"
)

func TestK8sSecretInjector_Inject(t *testing.T) {
	inputPod := corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "container1",
				},
			},
		},
	}

	inputPodWithVolume := corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "container1",
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:     "kube-api-access-gxmz8",
							ReadOnly: true,
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name:         "kube-api-access-gxmz8",
					VolumeSource: corev1.VolumeSource{},
				},
			},
		},
	}

	successPodEnv := corev1.Pod{
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{},
			Containers: []corev1.Container{
				{
					Name: "container1",
					Env: []corev1.EnvVar{
						{
							Name: "_FSEC_GROUP_HELLO",
							ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									Key: "hello",
									LocalObjectReference: corev1.LocalObjectReference{
										Name: "group",
									},
								},
							},
						},
						{
							Name:  "FLYTE_SECRETS_ENV_PREFIX",
							Value: "_FSEC_",
						},
					},
				},
			},
		},
	}

	successPodFile := corev1.Pod{
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: "m4zg54lql3",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: "group",
							Items: []corev1.KeyToPath{
								{
									Key:  "hello",
									Path: "hello",
								},
							},
						},
					},
				},
			},
			InitContainers: []corev1.Container{},
			Containers: []corev1.Container{
				{
					Name: "container1",
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "m4zg54lql3",
							MountPath: "/etc/flyte/secrets/group",
							ReadOnly:  true,
						},
					},
					Env: []corev1.EnvVar{
						{
							Name:  "FLYTE_SECRETS_DEFAULT_DIR",
							Value: "/etc/flyte/secrets",
						},
						{
							Name: "FLYTE_SECRETS_FILE_PREFIX",
						},
					},
				},
			},
		},
	}

	successPodMultiFiles := corev1.Pod{
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: "m4zg54lql3",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: "group",
							Items: []corev1.KeyToPath{
								{
									Key:  "hello",
									Path: "hello",
								},
								{
									Key:  "world",
									Path: "world",
								},
							},
						},
					},
				},
			},
			InitContainers: []corev1.Container{},
			Containers: []corev1.Container{
				{
					Name: "container1",
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "m4zg54lql3",
							MountPath: "/etc/flyte/secrets/group",
							ReadOnly:  true,
						},
					},
					Env: []corev1.EnvVar{
						{
							Name:  "FLYTE_SECRETS_DEFAULT_DIR",
							Value: "/etc/flyte/secrets",
						},
						{
							Name: "FLYTE_SECRETS_FILE_PREFIX",
						},
					},
				},
			},
		},
	}

	successPodFileAllKeys := corev1.Pod{
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: "hello",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: "hello",
						},
					},
				},
			},
			InitContainers: []corev1.Container{},
			Containers: []corev1.Container{
				{
					Name: "container1",
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "hello",
							MountPath: "/etc/flyte/secrets/hello",
							ReadOnly:  true,
						},
					},
					Env: []corev1.EnvVar{
						{
							Name:  "FLYTE_SECRETS_DEFAULT_DIR",
							Value: "/etc/flyte/secrets",
						},
						{
							Name: "FLYTE_SECRETS_FILE_PREFIX",
						},
					},
				},
			},
		},
	}

	ctx := context.Background()
	type args struct {
		secret *core.Secret
		p      *corev1.Pod
	}
	tests := []struct {
		name    string
		args    args
		want    *corev1.Pod
		wantErr bool
	}{
		{name: "require group", args: args{secret: &coreIdl.Secret{Key: "hello", MountRequirement: coreIdl.Secret_ENV_VAR}, p: &corev1.Pod{}},
			want: &corev1.Pod{}, wantErr: true},
		{name: "simple", args: args{secret: &coreIdl.Secret{Group: "group", Key: "hello", MountRequirement: coreIdl.Secret_ENV_VAR}, p: inputPod.DeepCopy()},
			want: &successPodEnv, wantErr: false},
		{name: "require file on pod with volume", args: args{secret: &coreIdl.Secret{Group: "group", Key: "hello", MountRequirement: coreIdl.Secret_FILE}, p: inputPodWithVolume.DeepCopy()},
			want: &successPodEnv, wantErr: false},
		{name: "require file single", args: args{secret: &coreIdl.Secret{Group: "group", Key: "hello", MountRequirement: coreIdl.Secret_FILE},
			p: inputPod.DeepCopy()},
			want: &successPodFile, wantErr: false},
		{name: "require file multiple from same secret group", args: args{secret: &coreIdl.Secret{Group: "group", Key: "world", MountRequirement: coreIdl.Secret_FILE},
			p: successPodFile.DeepCopy()},
			want: &successPodMultiFiles, wantErr: false},
		{name: "require file all keys", args: args{secret: &coreIdl.Secret{Key: "hello", MountRequirement: coreIdl.Secret_FILE},
			p: inputPod.DeepCopy()},
			want: &successPodFileAllKeys, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := K8sSecretInjector{}
			got, _, err := i.Inject(ctx, tt.args.secret, tt.args.p)
			if (err != nil) != tt.wantErr {
				t.Errorf("Inject() error = %v, wantErr %v", err, tt.wantErr)
				return
			} else if err != nil {
				return
			}

			if diff := deep.Equal(got, tt.want); diff != nil {
				t.Errorf("Inject() Diff = %v\r\n got = %v\r\n want = %v", diff, got, tt.want)
			}
		})
	}
}
