package secrets

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
)

const annotationPrefix = "flyte.secrets"

// Copied from:
// https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/apimachinery/pkg/api/validation/objectmeta.go#L36
const totalAnnotationSizeLimitB int = 256 * (1 << 10) // 256 kB
const valueSeparator = "/"
const groupSeparator = "."

func encodeSecretGroup(group string) string {
	return strings.ToLower(base64.StdEncoding.EncodeToString([]byte(group)))
}

func decodeSecretGroup(encoded string) string {
	decodedRaw, err := base64.StdEncoding.DecodeString(strings.ToUpper(encoded))
	if err != nil {
		return encoded
	}

	return string(decodedRaw)
}

func MarshalSecretsToMapStrings(secrets []*core.Secret) (map[string]string, error) {
	res := make(map[string]string, len(secrets))
	for _, s := range secrets {
		if len(s.Group)+len(s.Key)+len(annotationPrefix) >= totalAnnotationSizeLimitB {
			return nil, fmt.Errorf("secret name cannot exceet [%v]", totalAnnotationSizeLimitB-len(annotationPrefix))
		}

		if len(s.Group) > 0 {
			res[annotationPrefix+groupSeparator+encodeSecretGroup(s.Group)+valueSeparator+s.Key] = s.MountRequirement.String()
		} else {
			res[annotationPrefix+valueSeparator+s.Key] = s.MountRequirement.String()
		}
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

			// Remove group separator if it exists...
			trimmed = strings.TrimPrefix(trimmed, groupSeparator)

			parts := strings.Split(trimmed, valueSeparator)

			res = append(res, &core.Secret{
				Group:            decodeSecretGroup(parts[0]),
				Key:              parts[1],
				MountRequirement: core.Secret_MountType(mountType),
			})
		}
	}

	return res, nil
}
