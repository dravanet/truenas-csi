package config

import (
	"fmt"
)

// CSIConfiguration
type CSIConfiguration map[string]*FreeNAS

// FreeNAS API access parameters
type FreeNAS struct {
	APIUrl string `yaml:"apiurl"`

	// FreeNAS : User & password
	Username string `yaml:"username,omitempty"`
	Password string `yaml:"password,omitempty"`

	// TrueNAS Core : ApiKey
	APIKey string `yaml:"apikey,omitempty"`

	// NFS holds global nfs configuration
	NFS *NFS `yaml:"nfs,omitempty"`

	// ISCSI holds global iSCSI configuration
	ISCSI *ISCSI `yaml:"iscsi,omitempty"`

	Configurations map[string]*Configuration `yaml:"configurations,omitempty"`

	name                  string
	rootDsToConfiguration map[string]*Configuration
}

// Name return NAS configuration's name
func (nas *FreeNAS) Name() string {
	return nas.name
}

// DeletePolicy specifies delete policy for a configuration
type DeletePolicy string

const (
	DeletePolicyDelete DeletePolicy = "delete"
	DeletePolicyRetain DeletePolicy = "retain"
)

// Configuration holds common configuration for nfs/iscsi shares
type Configuration struct {
	// RootDataset specifies the root dataset to allocate datasets under
	RootDataset string `yaml:"rootdataset"`

	// Sparse means to allocate sparse datasets/volumes (i.e. vithout refreservation)
	// Quota will be enforced always.
	Sparse bool `yaml:"sparse,omitempty"`

	// AllocateEnabled specifies whether allocation is enabled from this configuration
	AllocateEnabled bool `yaml:"allocateEnabled"`

	// DeletePolicy specifies delete policy for this configuration
	DeletePolicy DeletePolicy `yaml:"deletePolicy"`

	// NFS holds nfs configuration
	NFS *NFS `yaml:"nfs,omitempty"`

	// ISCSI holds iSCSI configuration
	ISCSI *ISCSI `yaml:"iscsi,omitempty"`
}

// NFS holds configuration for Filesystem Volumes
type NFS struct {
	Server string `yaml:"server"`

	AllowedHosts    []string `yaml:"allowedhosts"`
	AllowedNetworks []string `yaml:"allowednetworks"`
}

// ISCSI holds configuration for Block Volumes
type ISCSI struct {
	Portal   string `yaml:"portal"`
	PortalID int    `yaml:"portalid"`
}

// Validate validates configuration
func (cfg *CSIConfiguration) Validate() error {
	for name, nas := range *cfg {
		if err := nas.Validate(); err != nil {
			return err
		}

		nas.name = name
	}

	return nil
}

// Validate checks configuration that sane values are specifies.
// - performs uniqueness check among RootDatasets
func (nas *FreeNAS) Validate() error {
	nas.rootDsToConfiguration = make(map[string]*Configuration)

	for _, cfg := range nas.Configurations {
		if _, ok := nas.rootDsToConfiguration[cfg.RootDataset]; ok {
			return fmt.Errorf("RootDataset \"%s\" is duplicated in configuration", cfg.RootDataset)
		}

		if err := verifyDeletePolicy(cfg); err != nil {
			return err
		}

		// use global nfs/iscsi settings
		if cfg.NFS == nil {
			cfg.NFS = nas.NFS
		}
		if cfg.ISCSI == nil {
			cfg.ISCSI = nas.ISCSI
		}

		nas.rootDsToConfiguration[cfg.RootDataset] = cfg
	}

	return nil
}

func (cfg *FreeNAS) GetDeletePolicyForRootDataset(rootds string) DeletePolicy {
	return cfg.rootDsToConfiguration[rootds].DeletePolicy
}

func (cfg *FreeNAS) GetSparseForRootDataset(rootds string) bool {
	return cfg.rootDsToConfiguration[rootds].Sparse
}

func verifyDeletePolicy(c *Configuration) error {
	switch c.DeletePolicy {
	case DeletePolicyDelete, DeletePolicyRetain:
	default:
		return fmt.Errorf("Invalid deletePolicy specified")
	}

	return nil
}
