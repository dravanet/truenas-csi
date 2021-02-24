package node

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dravanet/truenas-csi/pkg/csi"
	"github.com/dravanet/truenas-csi/pkg/volumecontext"
)

func (ns *server) publishNFSVolume(ctx context.Context, req *csi.NodePublishVolumeRequest, nfs *volumecontext.NFS) error {
	cap := req.GetVolumeCapability()
	if cap == nil {
		return status.Errorf(codes.FailedPrecondition, "Volume capability not specified")
	}

	switch {
	case cap.GetMount() != nil:
		if err := execCmd(ctx, "mount", nfs.Address, req.TargetPath); err != nil {
			return status.Errorf(codes.Unavailable, "Error mounting filesystem: %+v", err)
		}
	default:
		return status.Errorf(codes.FailedPrecondition, "Invalid configuration requested")
	}

	return nil
}
