package secrets

import (
	"encoding/base32"
	"fmt"
	"strings"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
)

const annotationPrefix = "flyte.secrets"
const PodLabel = "inject-flyte-secrets"
const PodLabelValue = "true"

// Copied from:
// https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/apimachinery/pkg/api/validation/objectmeta.go#L36
const totalAnnotationSizeLimitB int = 256 * (1 << 10) // 256 kB
const valueSeparator = "/"
const groupSeparator = "."

var encoding = base32.StdEncoding.WithPadding(base32.NoPadding)

func encodeSecretGroup(group string) string {
	res := strings.ToLower(encoding.EncodeToString([]byte(group)))
	return strings.TrimSuffix(res, "=")
}

func decodeSecretGroup(encoded string) (string, error) {
	decodedRaw, err := encoding.DecodeString(strings.ToUpper(encoded))
	if err != nil {
		return encoded, err
	}

	return string(decodedRaw), nil
}

func MarshalSecretsToMapStrings(secrets []*core.Secret) (map[string]string, error) {
	res := make(map[string]string, len(secrets))
	for _, s := range secrets {
		if len(s.Group)+len(s.Key)+len(annotationPrefix) >= totalAnnotationSizeLimitB {
			return nil, fmt.Errorf("secret name cannot exceet [%v]", totalAnnotationSizeLimitB-len(annotationPrefix))
		}

		if _, found := core.Secret_MountType_name[int32(s.MountRequirement)]; !found {
			return nil, fmt.Errorf("invalid mount requirement [%v]", s.MountRequirement)
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

			decoded, err := decodeSecretGroup(parts[0])
			if err != nil {
				return nil, fmt.Errorf("error decoding [%v]. Error: %w", key, err)
			}

			res = append(res, &core.Secret{
				Group:            decoded,
				Key:              parts[1],
				MountRequirement: core.Secret_MountType(mountType),
			})
		}
	}

	return res, nil
}
