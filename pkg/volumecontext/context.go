package volumecontext

// VolumeContext represents data needed for a volume to be Staged/Published
type VolumeContext struct {
	// Dataset string `json:"dataset"`
	Iscsi *ISCSI `json:"iscsi,omitempty"`
	Nfs   *NFS   `json:"nfs,omitempty"`
}

// ISCSI represents ISCSI connection parameters
type ISCSI struct {
	Portal string `json:"portal"`
	Target string `json:"target"`

	InboundAuth  *ISCSIAuth `json:"inAuth,omitempty"`
	OutboundAuth *ISCSIAuth `json:"outAuth,omitempty"`
}

// ISCSIAuth holds ISCSI authentication parameters
type ISCSIAuth struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// NFS holds nfs parameters
type NFS struct {
	Address string `json:"address"`
}
