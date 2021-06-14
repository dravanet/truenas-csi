package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"path"

	"google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"

	"github.com/dravanet/truenas-csi/pkg/config"
	TruenasOapi "github.com/dravanet/truenas-csi/pkg/truenas"
	"github.com/dravanet/truenas-csi/pkg/volumecontext"
)

func (cs *server) createNFSVolume(ctx context.Context, cl *TruenasOapi.Client, nfs *config.NFS, reqName string, dataset string) (
	volumeContext *volumecontext.VolumeContext,
	err error) {

	paths := []string{path.Join("/mnt", dataset)}

	share, err := cs.getNFSShareByComment(ctx, cl, reqName)
	if err != nil {
		return
	}

	if share == nil {
		enabled := true
		maprootuser := "root"
		maprootgroup := "wheel"

		if _, err = handleNasResponse(cl.PostSharingNfs(ctx, TruenasOapi.PostSharingNfsJSONRequestBody{
			Enabled:      &enabled,
			Paths:        &paths,
			Comment:      &reqName,
			Hosts:        &nfs.AllowedHosts,
			Networks:     &nfs.AllowedNetworks,
			MaprootUser:  &maprootuser,
			MaprootGroup: &maprootgroup,
		})); err != nil {
			return
		}
	} else {
		if len(share.Paths) != 1 {
			return nil, fmt.Errorf("share %q uses more paths", reqName)
		}

		if share.Paths[0] != paths[0] {
			return nil, fmt.Errorf("share %q uses dataset %q (expected: %q)", reqName, share.Paths[0], paths[0])
		}
	}

	volumeContext = &volumecontext.VolumeContext{
		Nfs: &volumecontext.NFS{
			Address: fmt.Sprintf("%s:%s", nfs.Server, paths[0]),
		},
	}

	return
}

func (cs *server) deleteNFSVolume(ctx context.Context, cl *TruenasOapi.Client, di *datasetInfo) error {
	// Lookup nfs share
	share, err := cs.getNFSShareByComment(ctx, cl, di.Comments)
	if err != nil {
		return err
	}

	if share != nil {
		// Delete nfs share
		if _, err = handleNasResponse(cl.DeleteSharingNfsIdId(ctx, *share.ID)); err != nil {
			return err
		}
	}

	return nil
}

type nfsShare struct {
	ID      *int     `json:"id"`
	Comment *string  `json:"comment"`
	Paths   []string `json:"paths"`
}

func (cs *server) getNFSShareByComment(ctx context.Context, cl *TruenasOapi.Client, comment string) (share *nfsShare, err error) {
	var nfsshareresp []byte
	if nfsshareresp, err = handleNasResponse(cl.GetSharingNfs(ctx, &TruenasOapi.GetSharingNfsParams{}, truenasOapiFilter("comment", comment))); err != nil {
		return
	}
	var shares []nfsShare
	if err = json.Unmarshal(nfsshareresp, &shares); err != nil {
		return nil, status.Error(codes.Unavailable, "Error parsing NAS response")
	}
	if len(shares) > 1 {
		return nil, status.Errorf(codes.Unavailable, "Unexpected data returned from NAS, multiple items have same comment: %+v", shares)
	}
	if len(shares) == 1 {
		if shares[0].Comment == nil || *shares[0].Comment != comment {
			return nil, status.Errorf(codes.Unavailable, "Unexpected data returned from NAS, seems like filtering does not work: %+v", shares)
		}
	}

	if len(shares) == 1 {
		share = &shares[0]
	}

	return
}
