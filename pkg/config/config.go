package config

import (
	"fmt"
)

// Configuration
type Configuration map[string]*FreeNAS

// FreeNAS API access parameters
type FreeNAS struct {
	APIUrl string `yaml:"apiurl"`

	// FreeNAS : User & password
	Username string `yaml:"username,omitempty"`
	Password string `yaml:"password,omitempty"`

	// TrueNAS Core : ApiKey
	APIKey string `yaml:"apikey,omitempty"`

	// NFS Parameters
	NFS map[string]*NFS `yaml:"nfs,omitempty"`

	// ISCSI Parameters
	ISCSI map[string]*ISCSI `yaml:"iscsi,omitempty"`

	name                 string
	rootDsToDeletePolicy map[string]*DeletePolicy
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

// Validate validates configuration
func (cfg *Configuration) Validate() error {
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
	nas.rootDsToDeletePolicy = make(map[string]*DeletePolicy)

	for _, nfs := range nas.NFS {
		if _, ok := nas.rootDsToDeletePolicy[nfs.RootDataset]; ok {
			return fmt.Errorf("RootDataset \"%s\" is duplicated in configuration", nfs.RootDataset)
		}

		if err := verifyDeletePolicy(&nfs.Common); err != nil {
			return err
		}

		nas.rootDsToDeletePolicy[nfs.RootDataset] = nfs.DeletePolicy
	}

	for _, iscsi := range nas.ISCSI {
		if _, ok := nas.rootDsToDeletePolicy[iscsi.RootDataset]; ok {
			return fmt.Errorf("RootDataset \"%s\" is duplicated in configuration", iscsi.RootDataset)
		}

		if err := verifyDeletePolicy(&iscsi.Common); err != nil {
			return err
		}

		nas.rootDsToDeletePolicy[iscsi.RootDataset] = iscsi.DeletePolicy
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
