package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"

	"github.com/dravanet/truenas-csi/pkg/config"
	"github.com/dravanet/truenas-csi/pkg/csi"
	FreenasOapi "github.com/dravanet/truenas-csi/pkg/freenas"
)

type server struct {
	freenas *config.FreeNAS

	csi.UnimplementedControllerServer
}

const (
	volTypeKey   = "type"
	volTypeNFS   = "nfs"
	volTypeISCSI = "iscsi"
)

func (cs *server) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: []*csi.ControllerServiceCapability{
			{
				Type: &csi.ControllerServiceCapability_Rpc{
					Rpc: &csi.ControllerServiceCapability_RPC{
						Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
					},
				},
			},
			{
				Type: &csi.ControllerServiceCapability_Rpc{
					Rpc: &csi.ControllerServiceCapability_RPC{
						Type: csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
					},
				},
			},
		},
	}, nil
}

func (cs *server) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if req.CapacityRange.GetRequiredBytes() == 0 && req.CapacityRange.GetLimitBytes() == 0 {
		return nil, status.Error(codes.FailedPrecondition, "No capacity requirements specified")
	}

	// According to req.VolumeCapabilities, we must choose between nfs or iscsi
	var iscsi bool
	var nfs bool

	for _, cap := range req.VolumeCapabilities {
		switch {
		case cap.GetBlock() != nil:
			iscsi = true
		case cap.GetMount() != nil:
			if am := cap.GetAccessMode(); am != nil {
				switch am.GetMode() {
				case csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY:
					iscsi = true
				default:
					nfs = true
				}
			}
		}
	}

	// if we end up with conflicting request, return an error
	if iscsi && nfs {
		return nil, status.Error(codes.FailedPrecondition, "conflicting options")
	}

	switch {
	case nfs:
		return cs.createNFSVolume(ctx, req)
	case iscsi:
		return cs.createISCSIVolume(ctx, req)
	}

	return nil, status.Error(codes.FailedPrecondition, "Invalid VolumeCapabilities requested")
}

func (cs *server) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	var err error

	switch {
	case strings.HasPrefix(req.VolumeId, "nfs:"):
		err = cs.deleteNFSVolume(ctx, req)
	case strings.HasPrefix(req.VolumeId, "iscsi:"):
		err = cs.deleteISCSIVolume(ctx, req)
	default:
		err = status.Error(codes.FailedPrecondition, "Invalid volume id requested")
	}

	if err != nil {
		return nil, err
	}

	return &csi.DeleteVolumeResponse{}, nil
}

// ValidateVolumeCapabilities validates request.
func (cs *server) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	iscsi := strings.HasPrefix(req.VolumeId, "iscsi:")
	if !iscsi {
		if !strings.HasPrefix(req.VolumeId, "nfs:") {
			return nil, status.Error(codes.InvalidArgument, "Invalid volume id requested")
		}
	}

	for _, cap := range req.VolumeCapabilities {
		switch {
		case cap.GetBlock() != nil:
			if !iscsi {
				return &csi.ValidateVolumeCapabilitiesResponse{}, nil
			}
		case cap.GetMount() != nil:
			switch cap.GetAccessMode().GetMode() {
			case csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY:
			default:
				if iscsi { // multi-node access with ISCSI is not allowed
					return &csi.ValidateVolumeCapabilitiesResponse{}, nil
				}
			}
		}
	}

	return &csi.ValidateVolumeCapabilitiesResponse{Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
		VolumeContext:      req.VolumeContext,
		VolumeCapabilities: req.VolumeCapabilities,
		Parameters:         req.Parameters,
	}}, nil
}

// Expand volume
func (cs *server) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	switch {
	case strings.HasPrefix(req.VolumeId, "nfs:"):
		return cs.expandNFSVolume(ctx, req)
	case strings.HasPrefix(req.VolumeId, "iscsi:"):
		return cs.expandISCSIVolume(ctx, req)
	default:
		return nil, status.Error(codes.FailedPrecondition, "Invalid volume id requested")
	}
}

// New returns a new csi.ControllerServer
func New(cfg *config.FreeNAS) csi.ControllerServer {
	return &server{
		freenas: cfg,
	}
}

func newFreenasOapiClient(cfg *config.FreeNAS) (*FreenasOapi.Client, error) {
	var opts []FreenasOapi.ClientOption

	if cfg.APIKey != "" {
		opts = append(opts, FreenasOapi.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", cfg.APIKey))

			return nil
		}))
	} else {
		opts = append(opts, FreenasOapi.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
			req.SetBasicAuth(cfg.Username, cfg.Password)

			return nil
		}))
	}

	return FreenasOapi.NewClient(cfg.APIUrl, opts...)
}

// removeDataset removes or renames given dataset
// TODO: implement rename
func (cs *server) removeDataset(ctx context.Context, cl *FreenasOapi.Client, dataset string) error {
	recursive := true
	if _, err := handleNasResponse(cl.DeletePoolDatasetIdId(ctx, dataset, FreenasOapi.DeletePoolDatasetIdIdJSONRequestBody{Recursive: &recursive})); err != nil {
		return err
	}

	return nil
}

func handleNasResponse(resp *http.Response, err error) ([]byte, error) {
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "Error during call to Nas: %+v", err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "Error reading response body: %+v", err)
	}
	if resp.StatusCode != 200 {
		return nil, status.Errorf(codes.Unavailable, "Unexpected result from Nas: %s", string(body))
	}

	return body, nil
}

func handleNasCreateResponse(resp *http.Response, err error) (int, error) {
	body, err := handleNasResponse(resp, err)

	if err != nil {
		return 0, err
	}

	id, err := extractID(body)
	if err != nil {
		return 0, err
	}

	return id, nil
}

func extractID(data []byte) (int, error) {
	var result struct {
		ID *int `json:"id"`
	}

	if err := json.Unmarshal(data, &result); err != nil {
		return 0, err
	}

	if result.ID == nil {
		return 0, fmt.Errorf("No \"id\" key found")
	}

	return *result.ID, nil
}
