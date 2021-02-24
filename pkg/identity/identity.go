package identity

import (
	"context"

	"github.com/dravanet/truenas-csi/pkg/csi"
)

var (
	driver  = "github.com/dravanet/truenas-csi"
	version = "0.0.0"
)

type server struct {
	csi.UnsafeIdentityServer

	capabilitities []*csi.PluginCapability
}

func (is *server) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          driver,
		VendorVersion: version,
	}, nil
}

func (is *server) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: is.capabilitities,
	}, nil
}

func (is *server) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}

// New returns a new csi.IdentityServer
func New() csi.IdentityServer {
	is := &server{
		capabilitities: []*csi.PluginCapability{
			{
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
					},
				},
			},
			{
				Type: &csi.PluginCapability_VolumeExpansion_{
					VolumeExpansion: &csi.PluginCapability_VolumeExpansion{
						Type: csi.PluginCapability_VolumeExpansion_ONLINE,
					},
				},
			},
		},
	}

	return is
}
