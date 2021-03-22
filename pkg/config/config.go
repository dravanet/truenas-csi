package config

// FreeNAS API access parameters
type FreeNAS struct {
	APIUrl string `json:"apiurl"`

	// FreeNAS : User & password
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`

	// TrueNAS Core : ApiKey
	APIKey string `json:"apikey,omitempty"`

	// NFS Parameters
	NFS []*NFS `json:"nfs,omitempty"`

	// ISCSI Parameters
	ISCSI []*ISCSI `json:"iscsi,omitempty"`

	// Labels
	Labels map[string]string `json:"labels,omitempty"`
}

// NFS holds configuration for Filesystem Volumes
type NFS struct {
	Server string `json:"server"`

	RootDataset     string   `json:"rootdataset"`
	AllowedHosts    []string `json:"allowedhosts,omitempty"`
	AllowedNetworks []string `json:"allowednetworks,omitempty"`
}

// ISCSI holds configuration for Block Volumes
type ISCSI struct {
	Portal      string `json:"portal"`
	PortalID    int    `json:"portalid"`
	RootDataset string `json:"rootdataset"`
	Sparse      bool   `json:"sparse,omitempty"`
}
