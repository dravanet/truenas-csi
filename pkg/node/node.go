package node

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"

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
	targetPathInfo, err := os.Stat(req.TargetPath)
	if err != nil {
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	if targetPathInfo.IsDir() {
		if err := execCmd(ctx, "umount", req.TargetPath); err != nil {
			return nil, status.Errorf(codes.Unavailable, "Umount failed: %+v", err)
		}
	}

	os.Remove(req.TargetPath)

	return &csi.NodeUnpublishVolumeResponse{}, nil
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

func (ns *server) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	return ns.iscsiNodeExpandVolume(ctx, req)
}

// New returns csi.NodeServer
func New(nodeId string) csi.NodeServer {
	return &server{nodeId: nodeId}
}

func execCmd(ctx context.Context, name string, arg ...string) error {
	var stderr bytes.Buffer

	cmd := exec.CommandContext(ctx, name, arg...)
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("Failed running %s: %+v; %s", name, err, string(stderr.Bytes()))
	}

	return nil
}

func (ns *server) extractVolumeContext(volumeContext map[string]string) (*volumecontext.VolumeContext, error) {
	if b64, ok := volumeContext["b64"]; ok {
		return volumecontext.Base64Serializer().Deserialize(b64)
	}

	return nil, fmt.Errorf("VolumeContext did not contain expected field")
}
