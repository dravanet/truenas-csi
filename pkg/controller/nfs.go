package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"

	"github.com/dravanet/truenas-csi/pkg/config"
	"github.com/dravanet/truenas-csi/pkg/csi"
	FreenasOapi "github.com/dravanet/truenas-csi/pkg/freenas"
	"github.com/dravanet/truenas-csi/pkg/volumecontext"
)

func (cs *server) createNFSVolume(ctx context.Context, req *csi.CreateVolumeRequest) (resp *csi.CreateVolumeResponse, err error) {
	nfs := cs.findNFSconfiguration(req)

	if nfs == nil {
		return nil, status.Error(codes.InvalidArgument, "could not find a suitable nfs configuration")
	}

	cl, err := newFreenasOapiClient(cs.freenas)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "creating FreenasOapi client failed")
	}

	refreservation := int(req.CapacityRange.GetRequiredBytes())
	refquota := int(req.CapacityRange.GetLimitBytes())

	if refreservation == 0 {
		refreservation = refquota
	} else if refquota == 0 {
		refquota = refreservation
	}

	dataset := path.Join(nfs.RootDataset, req.Name)

	if _, err = handleNasResponse(cl.PostPoolDataset(ctx, FreenasOapi.PostPoolDatasetJSONRequestBody{
		Name:           &dataset,
		Refreservation: &refreservation,
		Refquota:       &refquota,
		Comments:       &req.Name,
	})); err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			recursive := true
			cl.DeletePoolDatasetIdId(ctx, dataset, FreenasOapi.DeletePoolDatasetIdIdJSONRequestBody{
				Recursive: &recursive,
			})
		}
	}()

	enabled := true
	paths := []string{path.Join("/mnt", dataset)}
	maprootuser := "root"
	maprootgroup := "wheel"

	var nfsID int
	if nfsID, err = handleNasCreateResponse(cl.PostSharingNfs(ctx, FreenasOapi.PostSharingNfsJSONRequestBody{
		Enabled:      &enabled,
		Paths:        &paths,
		Comment:      &req.Name,
		Hosts:        &nfs.AllowedHosts,
		Networks:     &nfs.AllowedNetworks,
		MaprootUser:  &maprootuser,
		MaprootGroup: &maprootgroup,
	})); err != nil {
		return nil, err
	}

	volumeContext := &volumecontext.VolumeContext{
		Nfs: &volumecontext.NFS{
			Address: fmt.Sprintf("%s:%s", nfs.Server, paths[0]),
		},
	}

	serialized, _ := volumecontext.Base64Serializer().Serialize(volumeContext)

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId: fmt.Sprintf("nfs:%d", nfsID),
			VolumeContext: map[string]string{
				"b64": serialized,
			},
		},
	}, nil
}

func (cs *server) deleteNFSVolume(ctx context.Context, req *csi.DeleteVolumeRequest) error {
	nfsID, err := strconv.ParseInt(strings.TrimPrefix(req.VolumeId, "nfs:"), 10, 32)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "Failed parsing nfsID: %+v", err)
	}

	cl, err := newFreenasOapiClient(cs.freenas)
	if err != nil {
		return status.Error(codes.Unavailable, "creating FreenasOapi client failed")
	}

	var nfsbody []byte
	if nfsbody, err = handleNasResponse(cl.GetSharingNfsIdId(ctx, []interface{}{nfsID}, &FreenasOapi.GetSharingNfsIdIdParams{})); err != nil {
		return err
	}

	var result struct {
		Paths []string `json:"paths"`
	}
	if err = json.Unmarshal(nfsbody, &result); err != nil {
		return status.Errorf(codes.InvalidArgument, "Failed parsing NAS response: %+v", err)
	}
	if len(result.Paths) != 1 {
		return status.Errorf(codes.InvalidArgument, "Unexpected response received for NFS share: id=%d result=%+v", nfsID, result)
	}

	if _, err = handleNasResponse(cl.DeleteSharingNfsIdId(ctx, int(nfsID))); err != nil {
		return err
	}

	return cs.removeDataset(ctx, cl, result.Paths[0])
}

func (cs *server) expandNFSVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	nfsID, err := strconv.ParseInt(strings.TrimPrefix(req.VolumeId, "nfs:"), 10, 32)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Failed parsing nfsID: %+v", err)
	}

	refreservation := int(req.CapacityRange.GetRequiredBytes())
	refquota := int(req.CapacityRange.GetLimitBytes())

	if refreservation == 0 {
		refreservation = refquota
	} else if refquota == 0 {
		refquota = refreservation
	}

	cl, err := newFreenasOapiClient(cs.freenas)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "creating FreenasOapi client failed")
	}

	var nfsbody []byte
	if nfsbody, err = handleNasResponse(cl.GetSharingNfsIdId(ctx, []interface{}{nfsID}, &FreenasOapi.GetSharingNfsIdIdParams{})); err != nil {
		return nil, err
	}

	var result struct {
		Paths []string `json:"paths"`
	}
	if err = json.Unmarshal(nfsbody, &result); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Failed parsing NAS response: %+v", err)
	}
	if len(result.Paths) != 1 {
		return nil, status.Errorf(codes.InvalidArgument, "Failed parsing NAS response: invalid Paths: %+v", result.Paths)
	}

	dataset := strings.TrimPrefix(result.Paths[0], "/mnt/")

	if _, err = handleNasResponse(cl.PutPoolDatasetIdId(ctx, dataset, FreenasOapi.PutPoolDatasetIdIdJSONRequestBody{
		Refreservation: &refreservation,
		Refquota:       &refquota,
	})); err != nil {
		return nil, err
	}

	return &csi.ControllerExpandVolumeResponse{CapacityBytes: int64(refreservation), NodeExpansionRequired: false}, nil
}

func (cs *server) findNFSconfiguration(req *csi.CreateVolumeRequest) *config.NFS {
	for _, nfs := range cs.freenas.NFS {
		return nfs
	}

	return nil
}
