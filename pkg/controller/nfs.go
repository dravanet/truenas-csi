package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"

	"github.com/dravanet/truenas-csi/pkg/config"
	"github.com/dravanet/truenas-csi/pkg/csi"
	FreenasOapi "github.com/dravanet/truenas-csi/pkg/freenas"
	"github.com/dravanet/truenas-csi/pkg/volumecontext"
)

func (cs *server) createNFSVolume(ctx context.Context, req *csi.CreateVolumeRequest, nas *config.FreeNAS) (
	dataset string,
	capacityBytes int64,
	volumeContext *volumecontext.VolumeContext,
	err error) {

	cl, err := newFreenasOapiClient(nas)
	if err != nil {
		err = status.Error(codes.Unavailable, "creating FreenasOapi client failed")
		return
	}

	nfs := cs.findNFSconfiguration(req, nas)
	if nfs == nil {
		err = status.Error(codes.InvalidArgument, "could not find a suitable nfs configuration")
		return
	}

	var paths []string

	capacityBytes = req.CapacityRange.GetRequiredBytes()
	if capacityBytes == 0 {
		capacityBytes = req.CapacityRange.GetLimitBytes()
	}

	share, err := cs.getNFSShareByComment(ctx, cl, req.Name)
	if err != nil {
		return
	}

	if share == nil {
		dataset = path.Join(nfs.RootDataset, uuid.NewString())

		refreservation := int(req.CapacityRange.GetRequiredBytes())
		refquota := int(req.CapacityRange.GetLimitBytes())

		if refreservation == 0 {
			refreservation = refquota
		} else if refquota == 0 {
			refquota = refreservation
		}

		if _, err = handleNasResponse(cl.PostPoolDataset(ctx, FreenasOapi.PostPoolDatasetJSONRequestBody{
			Name:           &dataset,
			Refreservation: &refreservation,
			Refquota:       &refquota,
			Comments:       &req.Name,
		})); err != nil {
			return
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
		paths = []string{path.Join("/mnt", dataset)}
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
			return
		}
		defer func() {
			if err != nil {
				cl.DeleteSharingNfsIdId(context.Background(), nfsID)
			}
		}()

		comments := fmt.Sprintf("nfs:%d", nfsID)
		if _, err = handleNasResponse(cl.PutPoolDatasetIdId(ctx, dataset, FreenasOapi.PutPoolDatasetIdIdJSONRequestBody{
			Comments: &comments,
		})); err != nil {
			return
		}
	} else {
		if share.Paths == nil || len(*share.Paths) != 1 {
			err = status.Errorf(codes.Unavailable, "Unexpected data returned from NAS, invalid paths: %+v", share)
			return
		}

		paths = *share.Paths
		dataset = strings.TrimPrefix(paths[0], "/mnt/")

		var ds *datasetInfo
		ds, err = cs.getDataset(ctx, cl, dataset)
		if err != nil {
			return
		}

		if ds == nil {
			err = status.Error(codes.Internal, "Dataset not found")
			return
		}

		if ds.Refreservation != nil {
			if capacityBytes != *ds.Refreservation {
				err = status.Error(codes.AlreadyExists, "Creating existing volume with different capacity")
				return
			}
		}
	}

	volumeContext = &volumecontext.VolumeContext{
		Nfs: &volumecontext.NFS{
			Address: fmt.Sprintf("%s:%s", nfs.Server, paths[0]),
		},
	}

	return
}

func (cs *server) deleteNFSVolume(ctx context.Context, cl *FreenasOapi.Client, di *datasetInfo) error {
	nfsID, err := strconv.ParseInt(strings.TrimPrefix(di.Comments, "nfs:"), 10, 32)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "Failed parsing nfsID: %+v", err)
	}

	if _, err = handleNasResponse(cl.DeleteSharingNfsIdId(ctx, int(nfsID))); err != nil {
		return err
	}

	return nil
}

func (cs *server) getNFSShareByComment(ctx context.Context, cl *FreenasOapi.Client, comment string) (share *FreenasOapi.PostSharingNfsJSONBody, err error) {
	var nfsshareresp []byte
	if nfsshareresp, err = handleNasResponse(cl.GetSharingNfs(ctx, &FreenasOapi.GetSharingNfsParams{}, freenasOapiFilter("comment", comment))); err != nil {
		return
	}
	var shares []FreenasOapi.PostSharingNfsJSONBody
	if err = json.Unmarshal(nfsshareresp, &shares); err != nil {
		return nil, status.Error(codes.Unavailable, "Error parsing NAS response")
	}
	for _, share := range shares {
		if share.Comment == nil || *share.Comment != comment {
			return nil, status.Errorf(codes.Unavailable, "Unexpected data returned from NAS, seems like filtering does not work: %+v", shares)
		}
	}
	if len(shares) > 1 {
		return nil, status.Errorf(codes.Unavailable, "Unexpected data returned from NAS, multiple items have same comment: %+v", shares)
	}

	if len(shares) == 1 {
		share = &shares[0]
	}

	return
}

func (cs *server) findNFSconfiguration(req *csi.CreateVolumeRequest, nas *config.FreeNAS) *config.NFS {
	configName := req.GetParameters()[configSelector]
	if configName == "" {
		configName = "default"
	}

	return nas.NFS[configName]
}
