package node

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dravanet/truenas-csi/pkg/csi"
	"github.com/dravanet/truenas-csi/pkg/volumecontext"
)

type server struct {
	nodeId string

	csi.UnimplementedNodeServer
}

var _ csi.NodeServer = &server{}

func (ns *server) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumeId not provided")
	}

	if req.StagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "StagingTargetPath not provided")
	}

	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "VolumeCapability not provided")
	}

	volumeContext, err := ns.extractVolumeContext(req.VolumeContext)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "Invalid volume context received: %+v", err)
	}

	if volumeContext.Iscsi != nil {
		if err = ns.stageISCSIVolume(ctx, req, volumeContext.Iscsi); err != nil {
			return nil, err
		}
	}

	return &csi.NodeStageVolumeResponse{}, nil
}

func (ns *server) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumeId not provided")
	}

	if req.StagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "StagingTargetPath not provided")
	}

	iscsiFile := path.Join(req.StagingTargetPath, "iscsi")

	if targetb, err := ioutil.ReadFile(iscsiFile); err == nil {
		ns.unstageISCSIVolume(ctx, string(targetb))
		rdevFile := path.Join(req.StagingTargetPath, "device")
		os.Remove(rdevFile)
		os.Remove(iscsiFile)
	}

	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (ns *server) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumeId not provided")
	}

	if req.TargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "TargetPath not provided")
	}

	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "VolumeCapability not provided")
	}

	if req.StagingTargetPath == "" {
		return nil, status.Error(codes.FailedPrecondition, "StagingTargetPath not set")
	}
	stagingPathInfo, err := os.Stat(req.StagingTargetPath)
	if err != nil || !stagingPathInfo.IsDir() {
		return nil, status.Error(codes.FailedPrecondition, "StagingTargetPath does not exist or not a directory")
	}

	volumeContext, err := ns.extractVolumeContext(req.VolumeContext)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "Invalid volume context received: %+v", err)
	}

	if volumeContext.Nfs != nil {
		err = ns.publishNFSVolume(ctx, req, volumeContext.Nfs)
	} else if volumeContext.Iscsi != nil {
		err = ns.publishISCSIVolume(ctx, req)
	}

	if err != nil {
		return nil, err
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *server) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumeId not provided")
	}

	if req.TargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "TargetPath not provided")
	}

	ismnt, _ := isMountPoint(req.TargetPath)
	if ismnt {
		if err := execCmd(ctx, "umount", req.TargetPath); err != nil {
			return nil, status.Errorf(codes.Unavailable, "Umount failed: %+v", err)
		}
	}

	os.Remove(req.TargetPath)

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *server) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	ismnt, stat := isMountPoint(req.VolumePath)

	if ismnt {
		var statfs unix.Statfs_t
		if err := unix.Statfs(req.VolumePath, &statfs); err != nil {
			return nil, status.Errorf(codes.Unavailable, "statfs failed on %q", req.VolumePath)
		}

		return &csi.NodeGetVolumeStatsResponse{
			Usage: []*csi.VolumeUsage{
				{
					Unit:      csi.VolumeUsage_BYTES,
					Total:     int64(statfs.Bsize) * int64(statfs.Blocks),
					Available: int64(statfs.Bsize) * int64(statfs.Bfree),
					Used:      int64(statfs.Bsize) * int64(statfs.Blocks-statfs.Bfree),
				},
				{
					Unit:      csi.VolumeUsage_INODES,
					Total:     int64(statfs.Files),
					Available: int64(statfs.Ffree),
					Used:      int64(statfs.Files - statfs.Ffree),
				},
			},
		}, nil
	} else if stat.Mode&unix.S_IFBLK == unix.S_IFBLK {
		dev, err := os.Open(req.VolumePath)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "failed opening device %q: %s", dev, err)
		}
		defer dev.Close()
		size, err := dev.Seek(0, 2)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "failed seeking to end of device %q: %s", dev, err)
		}

		return &csi.NodeGetVolumeStatsResponse{
			Usage: []*csi.VolumeUsage{
				{
					Unit:  csi.VolumeUsage_BYTES,
					Total: size,
				},
			},
		}, nil

	}

	return nil, status.Error(codes.Unavailable, "cannot return stat")
}

func (ns *server) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumeId not provided")
	}

	if req.VolumePath == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumePath not provided")
	}

	return ns.iscsiNodeExpandVolume(ctx, req)
}

func (ns *server) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
					},
				},
			},
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
					},
				},
			},
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
					},
				},
			},
		},
	}, nil
}

func (ns *server) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: ns.nodeId,
	}, nil
}

// New returns csi.NodeServer
func New(nodeId string) csi.NodeServer {
	return &server{nodeId: nodeId}
}

func execCmd(ctx context.Context, name string, arg ...string) error {
	cmd := exec.CommandContext(ctx, name, arg...)

	return cmd.Run()
}

func (ns *server) extractVolumeContext(volumeContext map[string]string) (*volumecontext.VolumeContext, error) {
	if b64, ok := volumeContext["b64"]; ok {
		return volumecontext.Base64Serializer().Deserialize(b64)
	}

	return nil, fmt.Errorf("VolumeContext did not contain expected field")
}

// isMountPoint returns true if path exists and is a mountpoint
func isMountPoint(path string) (ismnt bool, stat unix.Stat_t) {
	if err := unix.Stat(path, &stat); err != nil {
		return
	}

	if stat.Mode&unix.S_IFMT == unix.S_IFDIR {
		var parentStat unix.Stat_t
		if err := unix.Stat(filepath.Dir(path), &parentStat); err != nil {
			return
		}

		ismnt = stat.Dev != parentStat.Dev
	}

	return
}
