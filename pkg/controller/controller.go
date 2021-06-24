package controller

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"strings"

	"github.com/tv42/zbase32"
	"google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"

	"github.com/dravanet/truenas-csi/pkg/config"
	"github.com/dravanet/truenas-csi/pkg/csi"
	TruenasOapi "github.com/dravanet/truenas-csi/pkg/truenas"
	"github.com/dravanet/truenas-csi/pkg/volumecontext"
)

const (
	nasSelector    = "truenas-csi.dravanet.net/nas"
	configSelector = "truenas-csi.dravanet.net/config"
)

type server struct {
	config config.CSIConfiguration

	csi.UnimplementedControllerServer
}

func (cs *server) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "No name specified")
	}

	// from here req is not null

	if len(req.VolumeCapabilities) == 0 {
		return nil, status.Error(codes.InvalidArgument, "No VolumeCapabilities specified")
	}

	nasName := req.Parameters[nasSelector]
	if nasName == "" {
		nasName = "default"
	}
	nas := cs.config[nasName]
	if nas == nil {
		return nil, status.Errorf(codes.Unavailable, "No nas found with name %q", nasName)
	}

	cl, err := newTruenasOapiClient(nas)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "creating TruenasOapi client failed for %q", nasName)
	}

	configName := req.Parameters[configSelector]
	if configName == "" {
		configName = "default"
	}
	cfg := nas.Configurations[configName]
	if cfg == nil {
		return nil, status.Errorf(codes.Unavailable, "No configuration found with name %q", configName)
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
		return nil, status.Error(codes.InvalidArgument, "conflicting options")
	}

	if !iscsi && !nfs {
		return nil, status.Error(codes.InvalidArgument, "Invalid VolumeCapabilities requested")
	}

	// Calculate capacity
	capacityrange := req.CapacityRange
	if capacityrange == nil {
		// Default capacity of 1Gi
		capacityrange = &csi.CapacityRange{
			RequiredBytes: 1 << 30,
			LimitBytes:    1 << 30,
		}
	}

	capacityBytes := capacityrange.LimitBytes
	if capacityBytes == 0 {
		capacityBytes = capacityrange.RequiredBytes
	}

	// Prepare create request
	datasetName := datasetFromReqName(req.Name)
	dataset := path.Join(cfg.Dataset, datasetName)
	requestBody := TruenasOapi.PostPoolDatasetJSONRequestBody{
		Name:     &dataset,
		Comments: &req.Name,
	}

	if nfs {
		// filesystem case
		if cfg.NFS == nil {
			return nil, status.Errorf(codes.Unavailable, "cannot provision nfs share for %q", req.Name)
		}

		voltype := TruenasOapi.PoolDatasetCreate0TypeFILESYSTEM
		requestBody.Type = &voltype

		refreservation := int(capacityrange.RequiredBytes)
		refquota := int(capacityrange.LimitBytes)

		if refreservation == 0 {
			refreservation = refquota
		} else if refquota == 0 {
			refquota = refreservation
		}

		if refquota > 0 {
			requestBody.Refquota = &refquota
		}
		if refreservation > 0 && !cfg.Sparse {
			requestBody.Refreservation = &refreservation
		}
	} else {
		// iscsi case
		if cfg.ISCSI == nil {
			return nil, status.Errorf(codes.Unavailable, "cannot provision iscsi share for %q", req.Name)
		}

		voltype := TruenasOapi.PoolDatasetCreate0TypeVOLUME
		requestBody.Type = &voltype

		volsize := int(capacityBytes)
		requestBody.Volsize = &volsize
		requestBody.Sparse = &cfg.Sparse
	}

	createresp, err := cl.PostPoolDataset(ctx, requestBody)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "failed provisioning %q: %+v", req.Name, err)
	}

	switch createresp.StatusCode {
	case 200:
		// Create succeeded
		if nfs {
			// Set permissions to world-writable
			mode := "0777"
			if _, err = handleNasResponse(cl.PostPoolDatasetIdIdPermission(ctx, dataset, TruenasOapi.PostPoolDatasetIdIdPermissionJSONRequestBody{
				Acl:  &[]map[string]interface{}{},
				Mode: &mode,
			})); err != nil {
				return nil, status.Errorf(codes.Unavailable, "failed setting permissions on dataset for %q", req.Name)
			}
		}
	default:
		// Create failed due to conflict or other errors
		ds, err := cs.getDataset(ctx, cl, dataset)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "failed querying existing dataset %q: %+v", dataset, err)
		}

		if ds == nil {
			return nil, status.Errorf(codes.Unavailable, "failed querying existing dataset %q: not found", dataset)
		}

		if ds.Comments != req.Name {
			return nil, status.Errorf(codes.Unavailable, "dataset for %q exists with different comment, perhaps hash collision?", req.Name)
		}

		if (nfs && capacityBytes != *ds.Refquota) || (iscsi && capacityBytes != *ds.Volsize) {
			return nil, status.Errorf(codes.AlreadyExists, "capacity requirements changed for existing volume %q", req.Name)
		}
	}

	var volumeContext *volumecontext.VolumeContext

	switch {
	case nfs:
		volumeContext, err = cs.createNFSVolume(ctx, cl, cfg.NFS, req.Name, dataset)
	case iscsi:
		volumeContext, err = cs.createISCSIVolume(ctx, cl, cfg.ISCSI, req.Name, dataset, datasetName)
	default:
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

	// from here req is not null

	nas, dataset, err := cs.parsevolumeid(req.VolumeId)
	if err != nil {
		return &csi.DeleteVolumeResponse{}, nil
	}

	cl, err := newTruenasOapiClient(nas)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "creating FreenasOapi client failed")
	}

	var di *datasetInfo
	if di, err = cs.getDataset(ctx, cl, dataset); err != nil {
		return nil, status.Errorf(codes.Unavailable, "Error querying dataset: %+v", err)
	}
	if di != nil {
		dp := nas.GetDeletePolicyForRootDataset(path.Dir(dataset))

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

	// from here req is not null

	nas, dataset, err := cs.parsevolumeid(req.VolumeId)
	if err != nil {
		return nil, err
	}

	cl, err := newTruenasOapiClient(nas)
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
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "No VolumeId specified")
	}

	// from here req is not null

	nas, dataset, err := cs.parsevolumeid(req.VolumeId)
	if err != nil {
		return nil, err
	}

	cl, err := newTruenasOapiClient(nas)
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
	nodeExpansionRequired := false

	switch di.Type {
	case "FILESYSTEM":
		refreservation := int(req.CapacityRange.RequiredBytes)
		refquota := int(req.CapacityRange.LimitBytes)

		if refreservation == 0 {
			refreservation = refquota
		} else if refquota == 0 {
			refquota = refreservation
		}

		if _, err = handleNasResponse(cl.PutPoolDatasetIdId(ctx, di.ID, TruenasOapi.PutPoolDatasetIdIdJSONRequestBody{
			Refquota: &refquota,
		})); err != nil {
			return nil, err
		}

		if !nas.GetSparseForRootDataset(path.Dir(dataset)) {
			if _, err = handleNasResponse(cl.PutPoolDatasetIdId(ctx, di.ID, TruenasOapi.PutPoolDatasetIdIdJSONRequestBody{
				Refreservation: &refreservation,
			})); err != nil {
				return nil, err
			}
		}

		capacity = int64(refquota)

	case "VOLUME":
		volsize := int(req.CapacityRange.LimitBytes)
		if volsize == 0 {
			volsize = int(req.CapacityRange.RequiredBytes)
		}

		if _, err = handleNasResponse(cl.PutPoolDatasetIdId(ctx, di.ID, TruenasOapi.PutPoolDatasetIdIdJSONRequestBody{
			Volsize: &volsize,
		})); err != nil {
			return nil, err
		}

		capacity = int64(volsize)
		nodeExpansionRequired = true

	default:
		return nil, status.Errorf(codes.InvalidArgument, "Invalid dataset received from NAS: %+v", di)
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
func New(cfg config.CSIConfiguration) csi.ControllerServer {
	return &server{
		config: cfg,
	}
}

func newTruenasOapiClient(cfg *config.FreeNAS) (*TruenasOapi.Client, error) {
	var opts []TruenasOapi.ClientOption

	if cfg.APIKey != "" {
		opts = append(opts, TruenasOapi.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", cfg.APIKey))

			return nil
		}))
	} else {
		opts = append(opts, TruenasOapi.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
			req.SetBasicAuth(cfg.Username, cfg.Password)

			return nil
		}))
	}

	return TruenasOapi.NewClient(cfg.APIUrl, opts...)
}

func truenasOapiFilter(key, value string) func(ctx context.Context, req *http.Request) error {
	return func(ctx context.Context, req *http.Request) error {
		q := req.URL.Query()
		q.Add(key, value)
		req.URL.RawQuery = q.Encode()

		return nil
	}
}

type datasetInfo struct {
	ID       string
	Type     string
	Comments string
	Refquota *int64
	Volsize  *int64
}

func (cs *server) getDataset(ctx context.Context, cl *TruenasOapi.Client, dataset string) (*datasetInfo, error) {
	resp, err := cl.GetPoolDatasetIdId(ctx, dataset, &TruenasOapi.GetPoolDatasetIdIdParams{})
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
			Refquota *struct {
				Parsed int64 `json:"parsed"`
			} `json:"refquota"`
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
		if result.Refquota != nil {
			di.Refquota = &result.Refquota.Parsed
		}

		return di, nil
	case 404:
		return nil, nil
	}

	return nil, status.Errorf(codes.Unavailable, "Unexpected result from Nas: %s", string(body))
}

// removeDataset removes or renames given dataset
// TODO: implement rename
func (cs *server) removeDataset(ctx context.Context, cl *TruenasOapi.Client, dataset string, dp config.DeletePolicy) error {
	var err error

	if dp == config.DeletePolicyDelete {
		recursive := true

		_, err = handleNasResponse(cl.DeletePoolDatasetIdId(ctx, dataset, TruenasOapi.DeletePoolDatasetIdIdJSONRequestBody{Recursive: &recursive}))
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

func datasetFromReqName(reqName string) string {
	hashed := sha1.Sum([]byte(reqName))

	return zbase32.EncodeToString(hashed[:])
}
