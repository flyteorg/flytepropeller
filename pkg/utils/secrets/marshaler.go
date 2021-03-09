package secrets

import (
	"fmt"
	"strings"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
)

const annotationPrefix = "secrets.flyte/"

// Copied from:
// https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/apimachinery/pkg/api/validation/objectmeta.go#L36
const totalAnnotationSizeLimitB int = 256 * (1 << 10) // 256 kB

func MarshalSecretsToMapStrings(secrets []*core.Secret) (map[string]string, error) {
	res := make(map[string]string, len(secrets))
	for _, s := range secrets {
		if len(s.Name)+len(annotationPrefix) >= totalAnnotationSizeLimitB {
			return nil, fmt.Errorf("secret name cannot exceet [%v]", totalAnnotationSizeLimitB-len(annotationPrefix))
		}

		res[annotationPrefix+s.Name] = s.MountRequirement.String()
	}

	return res, nil
}

func UnmarshalStringMapToSecrets(m map[string]string) ([]*core.Secret, error) {
	res := make([]*core.Secret, 0, len(m))
	for key, val := range m {
		if trimmed := strings.TrimPrefix(key, annotationPrefix); len(trimmed) < len(key) {
			mountType, found := core.Secret_MountType_value[val]
			if !found {
				return nil, fmt.Errorf("failed to unmarshal secret [%v]'s mount type [%v]. Mount type not found",
					trimmed, val)
			}

			res = append(res, &core.Secret{
				Name:             trimmed,
				MountRequirement: core.Secret_MountType(mountType),
			})
		}
	}

	return res, nil
}
