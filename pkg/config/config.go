package config

import (
	"fmt"
)

// FreeNAS API access parameters
type FreeNAS struct {
	APIUrl string `yaml:"apiurl"`

	// FreeNAS : User & password
	Username string `yaml:"username,omitempty"`
	Password string `yaml:"password,omitempty"`

	// TrueNAS Core : ApiKey
	APIKey string `yaml:"apikey,omitempty"`

	// NFS Parameters
	NFS []*NFS `yaml:"nfs,omitempty"`

	// ISCSI Parameters
	ISCSI []*ISCSI `yaml:"iscsi,omitempty"`

	// Labels
	Labels map[string]string `yaml:"labels,omitempty"`

	rootDsToDeletePolicy map[string]*DeletePolicy
}

// DeletePolicy specifies delete policy for a configuration
type DeletePolicy string

const (
	DeletePolicyDelete DeletePolicy = "delete"
	DeletePolicyRetain DeletePolicy = "retain"
)

// Common holds common configuration for nfs/iscsi shares
type Common struct {
	// RootDataset specifies the root dataset to allocate datasets under
	RootDataset string `yaml:"rootdataset"`

	// AllocateEnabled specifies whether allocation is enabled from this configuration
	AllocateEnabled bool `yaml:"allocateEnabled"`

	// DeletePolicy specifies delete policy for this configuration
	DeletePolicy *DeletePolicy `yaml:"deletePolicy"`
}

// NFS holds configuration for Filesystem Volumes
type NFS struct {
	// common parameters
	Common `yaml:",inline"`

	Server string `yaml:"server"`

	AllowedHosts    []string `yaml:"allowedhosts"`
	AllowedNetworks []string `yaml:"allowednetworks"`
}

// ISCSI holds configuration for Block Volumes
type ISCSI struct {
	// common parameters
	Common `yaml:",inline"`

	Portal   string `yaml:"portal"`
	PortalID int    `yaml:"portalid"`
	Sparse   bool   `yaml:"sparse,omitempty"`
}

// Validate checks configuration that sane values are specifies.
// - performs uniqueness check among RootDatasets
func (cfg *FreeNAS) Validate() error {
	cfg.rootDsToDeletePolicy = make(map[string]*DeletePolicy)

	for _, nfs := range cfg.NFS {
		if _, ok := cfg.rootDsToDeletePolicy[nfs.RootDataset]; ok {
			return fmt.Errorf("RootDataset \"%s\" is duplicated in configuration", nfs.RootDataset)
		}

		if err := verifyDeletePolicy(&nfs.Common); err != nil {
			return err
		}

		cfg.rootDsToDeletePolicy[nfs.RootDataset] = nfs.DeletePolicy
	}

	for _, iscsi := range cfg.ISCSI {
		if _, ok := cfg.rootDsToDeletePolicy[iscsi.RootDataset]; ok {
			return fmt.Errorf("RootDataset \"%s\" is duplicated in configuration", iscsi.RootDataset)
		}

		if err := verifyDeletePolicy(&iscsi.Common); err != nil {
			return err
		}

		cfg.rootDsToDeletePolicy[iscsi.RootDataset] = iscsi.DeletePolicy
	}

	return nil
}

func (cfg *FreeNAS) GetDeletePolicyForRootDataset(rootds string) *DeletePolicy {
	return cfg.rootDsToDeletePolicy[rootds]
}

func verifyDeletePolicy(c *Common) error {
	if c.DeletePolicy == nil {
		return nil
	}

	switch *c.DeletePolicy {
	case DeletePolicyDelete, DeletePolicyRetain:
	default:
		return fmt.Errorf("Invalid deletePolicy specified")
	}

	return nil
}
