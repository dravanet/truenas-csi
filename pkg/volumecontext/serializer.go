package volumecontext

// Serializer can serialize and de-serialize a VolumeContext object
type Serializer interface {
	Serialize(*VolumeContext) (string, error)
	Deserialize(string) (*VolumeContext, error)
}
