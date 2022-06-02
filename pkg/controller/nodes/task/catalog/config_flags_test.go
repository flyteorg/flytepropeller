// Code generated by go generate; DO NOT EDIT.
// This file was generated by robots.

package catalog

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/mitchellh/mapstructure"
	"github.com/stretchr/testify/assert"
)

var dereferencableKindsConfig = map[reflect.Kind]struct{}{
	reflect.Array: {}, reflect.Chan: {}, reflect.Map: {}, reflect.Ptr: {}, reflect.Slice: {},
}

// Checks if t is a kind that can be dereferenced to get its underlying type.
func canGetElementConfig(t reflect.Kind) bool {
	_, exists := dereferencableKindsConfig[t]
	return exists
}

// This decoder hook tests types for json unmarshaling capability. If implemented, it uses json unmarshal to build the
// object. Otherwise, it'll just pass on the original data.
func jsonUnmarshalerHookConfig(_, to reflect.Type, data interface{}) (interface{}, error) {
	unmarshalerType := reflect.TypeOf((*json.Unmarshaler)(nil)).Elem()
	if to.Implements(unmarshalerType) || reflect.PtrTo(to).Implements(unmarshalerType) ||
		(canGetElementConfig(to.Kind()) && to.Elem().Implements(unmarshalerType)) {

		raw, err := json.Marshal(data)
		if err != nil {
			fmt.Printf("Failed to marshal Data: %v. Error: %v. Skipping jsonUnmarshalHook", data, err)
			return data, nil
		}

		res := reflect.New(to).Interface()
		err = json.Unmarshal(raw, &res)
		if err != nil {
			fmt.Printf("Failed to umarshal Data: %v. Error: %v. Skipping jsonUnmarshalHook", data, err)
			return data, nil
		}

		return res, nil
	}

	return data, nil
}

func decode_Config(input, result interface{}) error {
	config := &mapstructure.DecoderConfig{
		TagName:          "json",
		WeaklyTypedInput: true,
		Result:           result,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToSliceHookFunc(","),
			jsonUnmarshalerHookConfig,
		),
	}

	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}

	return decoder.Decode(input)
}

func join_Config(arr interface{}, sep string) string {
	listValue := reflect.ValueOf(arr)
	strs := make([]string, 0, listValue.Len())
	for i := 0; i < listValue.Len(); i++ {
		strs = append(strs, fmt.Sprintf("%v", listValue.Index(i)))
	}

	return strings.Join(strs, sep)
}

func testDecodeJson_Config(t *testing.T, val, result interface{}) {
	assert.NoError(t, decode_Config(val, result))
}

func testDecodeRaw_Config(t *testing.T, vStringSlice, result interface{}) {
	assert.NoError(t, decode_Config(vStringSlice, result))
}

func TestConfig_GetPFlagSet(t *testing.T) {
	val := Config{}
	cmdFlags := val.GetPFlagSet("")
	assert.True(t, cmdFlags.HasFlags())
}

func TestConfig_SetFlags(t *testing.T) {
	actual := Config{}
	cmdFlags := actual.GetPFlagSet("")
	assert.True(t, cmdFlags.HasFlags())

	t.Run("Test_type", func(t *testing.T) {

		t.Run("Override", func(t *testing.T) {
			testValue := "1"

			cmdFlags.Set("type", testValue)
			if vString, err := cmdFlags.GetString("type"); err == nil {
				testDecodeJson_Config(t, fmt.Sprintf("%v", vString), &actual.Type)

			} else {
				assert.FailNow(t, err.Error())
			}
		})
	})
	t.Run("Test_endpoint", func(t *testing.T) {

		t.Run("Override", func(t *testing.T) {
			testValue := "1"

			cmdFlags.Set("endpoint", testValue)
			if vString, err := cmdFlags.GetString("endpoint"); err == nil {
				testDecodeJson_Config(t, fmt.Sprintf("%v", vString), &actual.Endpoint)

			} else {
				assert.FailNow(t, err.Error())
			}
		})
	})
	t.Run("Test_insecure", func(t *testing.T) {

		t.Run("Override", func(t *testing.T) {
			testValue := "1"

			cmdFlags.Set("insecure", testValue)
			if vBool, err := cmdFlags.GetBool("insecure"); err == nil {
				testDecodeJson_Config(t, fmt.Sprintf("%v", vBool), &actual.Insecure)

			} else {
				assert.FailNow(t, err.Error())
			}
		})
	})
	t.Run("Test_max-cache-age", func(t *testing.T) {

		t.Run("Override", func(t *testing.T) {
			testValue := defaultConfig.MaxCacheAge.String()

			cmdFlags.Set("max-cache-age", testValue)
			if vString, err := cmdFlags.GetString("max-cache-age"); err == nil {
				testDecodeJson_Config(t, fmt.Sprintf("%v", vString), &actual.MaxCacheAge)

			} else {
				assert.FailNow(t, err.Error())
			}
		})
	})
	t.Run("Test_use-admin-auth", func(t *testing.T) {

		t.Run("Override", func(t *testing.T) {
			testValue := "1"

			cmdFlags.Set("use-admin-auth", testValue)
			if vBool, err := cmdFlags.GetBool("use-admin-auth"); err == nil {
				testDecodeJson_Config(t, fmt.Sprintf("%v", vBool), &actual.UseAdminAuth)

			} else {
				assert.FailNow(t, err.Error())
			}
		})
	})
	t.Run("Test_default-service-config", func(t *testing.T) {

		t.Run("Override", func(t *testing.T) {
			testValue := `{"loadBalancingConfig": [{"round_robin":{}}]}`

			cmdFlags.Set("default-service-config", testValue)
			if vString, err := cmdFlags.GetString("default-service-config"); err == nil {
				testDecodeJson_Config(t, fmt.Sprintf("%v", vString), &actual.DefaultServiceConfig)
			} else {
				assert.FailNow(t, err.Error())
			}
		})
	})
}
