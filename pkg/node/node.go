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
		iscsiFile := path.Join(req.StagingTargetPath, "iscsi")
		if _, err := os.Stat(iscsiFile); err != nil && os.IsNotExist(err) {
			if err = ns.stageISCSIVolume(ctx, req, volumeContext.Iscsi); err != nil {
				return nil, err
			}
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

	if _, err := os.Stat(req.TargetPath); err == nil {
		return &csi.NodePublishVolumeResponse{}, nil
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

	var targetPathInfo unix.Stat_t
	err := unix.Stat(req.TargetPath, &targetPathInfo)
	if err != nil {
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	if targetPathInfo.Mode&unix.S_IFDIR == unix.S_IFDIR {
		var parentInfo unix.Stat_t
		err = unix.Stat(filepath.Dir(req.TargetPath), &parentInfo)

		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "Stat parent failed: %+v", err)
		}

		if targetPathInfo.Dev != parentInfo.Dev {
			if err := execCmd(ctx, "umount", req.TargetPath); err != nil {
				return nil, status.Errorf(codes.Unavailable, "Umount failed: %+v", err)
			}
		}
	}

	os.Remove(req.TargetPath)

	return &csi.NodeUnpublishVolumeResponse{}, nil
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
