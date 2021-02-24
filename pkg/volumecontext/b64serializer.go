package volumecontext

import (
	"encoding/base64"
	"encoding/json"
)

// Base64Serializer returns a base64 serializer
func Base64Serializer() Serializer {
	return &base64Serializer{}
}

type base64Serializer struct {
}

func (b *base64Serializer) Serialize(v *VolumeContext) (string, error) {
	buf, err := json.Marshal(v)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(buf), nil
}

func (b *base64Serializer) Deserialize(s string) (*VolumeContext, error) {
	buf, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}

	var v VolumeContext

	if err = json.Unmarshal(buf, &v); err != nil {
		return nil, err
	}

	return &v, nil
}
