package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strings"

	"google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"

	"github.com/dravanet/truenas-csi/pkg/config"
	"github.com/dravanet/truenas-csi/pkg/csi"
	FreenasOapi "github.com/dravanet/truenas-csi/pkg/freenas"
	"github.com/dravanet/truenas-csi/pkg/volumecontext"
)

const (
	nasSelector    = "net.dravanet.truenas-csi/nas"
	configSelector = "net.dravanet.truenas-csi/config"
)

type server struct {
	config config.Configuration

	csi.UnimplementedControllerServer
}

func (cs *server) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "No name specified")
	}

	if req.CapacityRange.GetRequiredBytes() == 0 && req.CapacityRange.GetLimitBytes() == 0 {
		return nil, status.Error(codes.InvalidArgument, "No capacity requirements specified")
	}

	if len(req.VolumeCapabilities) == 0 {
		return nil, status.Error(codes.InvalidArgument, "No VolumeCapabilities specified")
	}

	nasName := req.GetParameters()[nasSelector]
	if nasName == "" {
		nasName = "default"
	}
	nas := cs.config[nasName]

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
		return nil, status.Error(codes.InvalidArgument, "conflicting options")
	}

	var err error
	var dataset string
	var capacityBytes int64
	var volumeContext *volumecontext.VolumeContext

	switch {
	case nfs:
		dataset, capacityBytes, volumeContext, err = cs.createNFSVolume(ctx, req, nas)
	case iscsi:
		dataset, capacityBytes, volumeContext, err = cs.createISCSIVolume(ctx, req, nas)
	default:
		return nil, status.Error(codes.InvalidArgument, "Invalid VolumeCapabilities requested")
	}

	if err != nil {
		return nil, err
	}

	serialized, _ := volumecontext.Base64Serializer().Serialize(volumeContext)

	volumeid := fmt.Sprintf("%s:%s", nas.Name(), dataset)

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			CapacityBytes: capacityBytes,
			VolumeId:      volumeid,
			VolumeContext: map[string]string{
				"b64": serialized,
			},
		},
	}, nil
}

func (cs *server) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "No VolumeId specified")
	}

	nas, dataset, err := cs.parsevolumeid(req.GetVolumeId())
	if err != nil {
		return &csi.DeleteVolumeResponse{}, nil
	}

	cl, err := newFreenasOapiClient(nas)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "creating FreenasOapi client failed")
	}

	var di *datasetInfo
	if di, err = cs.getDataset(ctx, cl, dataset); err != nil {
		return nil, status.Errorf(codes.Unavailable, "Error querying dataset: %+v", err)
	}
	if di != nil {
		dp := nas.GetDeletePolicyForRootDataset(path.Dir(dataset))

		if dp != nil {
			switch di.Type {
			case "FILESYSTEM":
				err = cs.deleteNFSVolume(ctx, cl, di)
			case "VOLUME":
				err = cs.deleteISCSIVolume(ctx, cl, di)
			default:
				err = status.Errorf(codes.InvalidArgument, "Received invalid response from NAS: %+v", di)
			}

			if err == nil {
				err = cs.removeDataset(ctx, cl, di.ID, dp)
			}
		} else {
			err = status.Errorf(codes.Internal, "Invalid internal configuration: no deletePolicy found")
		}
	}

	if err != nil {
		return nil, err
	}

	return &csi.DeleteVolumeResponse{}, nil
}

// ValidateVolumeCapabilities validates request.
func (cs *server) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if len(req.VolumeCapabilities) == 0 {
		return nil, status.Error(codes.InvalidArgument, "No VolumeCapabilities specified")
	}

	nas, dataset, err := cs.parsevolumeid(req.GetVolumeId())
	if err != nil {
		return nil, err
	}

	cl, err := newFreenasOapiClient(nas)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "creating FreenasOapi client failed")
	}

	di, err := cs.getDataset(ctx, cl, dataset)
	if err != nil {
		return nil, err
	}

	if di == nil {
		return nil, status.Errorf(codes.NotFound, "Volume does not exist")
	}

	for _, cap := range req.VolumeCapabilities {
		switch {
		case cap.GetBlock() != nil:
			if di.Type != "VOLUME" {
				return &csi.ValidateVolumeCapabilitiesResponse{}, nil
			}
		case cap.GetMount() != nil:
			switch cap.GetAccessMode().GetMode() {
			case csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY:
			default:
				if di.Type == "VOLUME" { // multi-node access with ISCSI is not allowed
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
	nas, dataset, err := cs.parsevolumeid(req.GetVolumeId())
	if err != nil {
		return nil, err
	}

	cl, err := newFreenasOapiClient(nas)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "creating FreenasOapi client failed")
	}

	di, err := cs.getDataset(ctx, cl, dataset)
	if err != nil {
		return nil, err
	}
	if di == nil {
		return nil, status.Errorf(codes.InvalidArgument, "Volume does not exist")
	}

	var capacity int64
	putrequest := FreenasOapi.PutPoolDatasetIdIdJSONRequestBody{}
	nodeExpansionRequired := false

	switch di.Type {
	case "FILESYSTEM":
		refreservation := int(req.CapacityRange.GetRequiredBytes())
		refquota := int(req.CapacityRange.GetLimitBytes())

		if refreservation == 0 {
			refreservation = refquota
		} else if refquota == 0 {
			refquota = refreservation
		}

		putrequest.Refreservation = &refreservation
		putrequest.Refquota = &refquota
		capacity = int64(refreservation)

	case "VOLUME":
		volsize := int(req.CapacityRange.GetLimitBytes())
		if volsize == 0 {
			volsize = int(req.CapacityRange.GetRequiredBytes())
		}

		putrequest.Volsize = &volsize
		capacity = int64(volsize)
		nodeExpansionRequired = true

	default:
		return nil, status.Errorf(codes.InvalidArgument, "Invalid dataset received from NAS: %+v", di)
	}

	if _, err = handleNasResponse(cl.PutPoolDatasetIdId(ctx, di.ID, putrequest)); err != nil {
		return nil, err
	}

	return &csi.ControllerExpandVolumeResponse{CapacityBytes: capacity, NodeExpansionRequired: nodeExpansionRequired}, nil
}

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

// New returns a new csi.ControllerServer
func New(cfg config.Configuration) csi.ControllerServer {
	return &server{
		config: cfg,
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

func freenasOapiFilter(key, value string) func(ctx context.Context, req *http.Request) error {
	return func(ctx context.Context, req *http.Request) error {
		q := req.URL.Query()
		q.Add(key, value)
		req.URL.RawQuery = q.Encode()

		return nil
	}
}

type datasetInfo struct {
	ID             string
	Type           string
	Comments       string
	Refreservation *int64
	Volsize        *int64
}

func (cs *server) getDataset(ctx context.Context, cl *FreenasOapi.Client, dataset string) (*datasetInfo, error) {
	resp, err := cl.GetPoolDatasetIdId(ctx, []interface{}{url.PathEscape(dataset)}, &FreenasOapi.GetPoolDatasetIdIdParams{})
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "Error during call to Nas: %+v", err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "Error reading response body: %+v", err)
	}

	switch resp.StatusCode {
	case 200:
		var result struct {
			ID       string `json:"id"`
			Type     string `json:"type"`
			Comments *struct {
				Rawvalue string `json:"rawvalue"`
			} `json:"comments"`
			Volsize *struct {
				Parsed int64 `json:"parsed"`
			} `json:"volsize"`
			Refreservation *struct {
				Parsed int64 `json:"parsed"`
			} `json:"refreservation"`
		}
		if err = json.Unmarshal(body, &result); err != nil {
			return nil, status.Errorf(codes.Unavailable, "Error parsing dataset from NAS: %+v", err)
		}

		di := &datasetInfo{
			ID:   result.ID,
			Type: result.Type,
		}
		if result.Comments != nil {
			di.Comments = result.Comments.Rawvalue
		}
		if result.Volsize != nil {
			di.Volsize = &result.Volsize.Parsed
		}
		if result.Refreservation != nil {
			di.Refreservation = &result.Refreservation.Parsed
		}

		return di, nil
	case 404:
		return nil, nil
	}

	return nil, status.Errorf(codes.Unavailable, "Unexpected result from Nas: %s", string(body))
}

// removeDataset removes or renames given dataset
// TODO: implement rename
func (cs *server) removeDataset(ctx context.Context, cl *FreenasOapi.Client, dataset string, dp *config.DeletePolicy) error {
	var err error

	if *dp == config.DeletePolicyDelete {
		recursive := true

		_, err = handleNasResponse(cl.DeletePoolDatasetIdId(ctx, dataset, FreenasOapi.DeletePoolDatasetIdIdJSONRequestBody{Recursive: &recursive}))
	}

	return err
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

func (cs *server) parsevolumeid(volumeid string) (nas *config.FreeNAS, dataset string, err error) {
	if volumeid == "" {
		err = status.Errorf(codes.InvalidArgument, "VolumeId not provided")
		return
	}

	parts := strings.SplitN(volumeid, ":", 2)

	if len(parts) != 2 {
		err = status.Errorf(codes.NotFound, "Invalid VolumeId received: %s", volumeid)
		return
	}

	nas = cs.config[parts[0]]

	if nas == nil {
		err = status.Errorf(codes.Unavailable, "NAS %s not found", parts[0])
		return
	}

	dataset = parts[1]

	return
}
