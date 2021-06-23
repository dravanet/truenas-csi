package node

import (
	"context"
	"os"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dravanet/truenas-csi/pkg/csi"
	"github.com/dravanet/truenas-csi/pkg/volumecontext"
)

func (ns *server) publishNFSVolume(ctx context.Context, req *csi.NodePublishVolumeRequest, nfs *volumecontext.NFS) error {
	switch {
	case req.VolumeCapability.GetMount() != nil:
		ismnt, _ := isMountPoint(req.TargetPath)
		if !ismnt {
			os.Mkdir(req.TargetPath, 0o755)
			if err := execCmd(ctx, "mount", nfs.Address, req.TargetPath); err != nil {
				return status.Errorf(codes.Unavailable, "Error mounting filesystem: %+v", err)
			}
		}
	default:
		return status.Errorf(codes.FailedPrecondition, "Invalid configuration requested")
	}

	return nil
}
