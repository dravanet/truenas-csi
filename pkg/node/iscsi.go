package node

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dravanet/truenas-csi/pkg/csi"
	"github.com/dravanet/truenas-csi/pkg/volumecontext"
)

func (ns *server) stageISCSIVolume(ctx context.Context, req *csi.NodeStageVolumeRequest, iscsi *volumecontext.ISCSI) (err error) {
	if err = iscsiAddNode(ctx, iscsi); err != nil {
		return status.Errorf(codes.Unavailable, "failed adding iscsi node: %+v", err)
	}
	defer func() {
		if err != nil {
			iscsiDeleteNode(context.Background(), iscsi.Target)
		}
	}()

	if err = iscsiLoginNode(ctx, iscsi.Target); err != nil {
		return status.Errorf(codes.Unavailable, "failed logging into iscsi target: %+v", err)
	}
	defer func() {
		if err != nil {
			iscsiLogoutNode(context.Background(), iscsi.Target)
		}
	}()

	rdev, err := iscsiWaitForDevice(ctx, iscsi.Portal, iscsi.Target)
	if err != nil {
		return status.Errorf(codes.Unavailable, "Error waiting for device: %+v", err)
	}

	if err = ioutil.WriteFile(path.Join(req.StagingTargetPath, "iscsi"), []byte(iscsi.Target), 0o640); err != nil {
		return status.Errorf(codes.Unavailable, "Error writing to staging/iscsi: %+v", err)
	}

	devicePath := path.Join(req.StagingTargetPath, "device")
	if err = os.Symlink(rdev, devicePath); err != nil {
		return status.Errorf(codes.Unavailable, "Error creating symlink: %+v", err)
	}

	if mount := req.GetVolumeCapability().GetMount(); mount != nil {
		blkidErr := execCmd(ctx, "blkid", "-p", devicePath)
		if blkidErr != nil {
			if exitError, ok := blkidErr.(*exec.ExitError); ok && exitError.ExitCode() == 2 {
				fsType := mount.FsType
				switch fsType {
				case "ext3", "ext4", "xfs":
				default:
					fsType = "ext4"
				}

				mkfsErr := execCmd(ctx, fmt.Sprintf("mkfs.%s", fsType), devicePath)
				if mkfsErr != nil {
					return status.Errorf(codes.Unavailable, "Error creating filesystem: %+v", err)
				}
			} else {
				return status.Errorf(codes.Unavailable, "Error determining existing filesystem: %+v", err)
			}
		}
	}

	return nil
}

func (ns *server) unstageISCSIVolume(ctx context.Context, target string) (err error) {
	if err = iscsiLogoutNode(ctx, target); err != nil {
		return
	}

	return iscsiDeleteNode(ctx, target)
}

func (ns *server) publishISCSIVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) error {
	cap := req.GetVolumeCapability()
	if cap == nil {
		return status.Errorf(codes.FailedPrecondition, "Volume capability not specified")
	}

	switch {
	case cap.GetBlock() != nil:
		device, err := os.Readlink(path.Join(req.StagingTargetPath, "device"))
		if err != nil {
			return status.Errorf(codes.Unavailable, "error reading symlink")
		}
		if err = os.Symlink(device, req.TargetPath); err != nil {
			return status.Errorf(codes.Unavailable, "Failed creating symlink at TargetPath")
		}
	case cap.GetMount() != nil:
		if err := execCmd(ctx, "mount", path.Join(req.StagingTargetPath, "device"), req.TargetPath); err != nil {
			return status.Errorf(codes.Unavailable, "Error mounting filesystem: %+v", err)
		}
	default:
		return status.Errorf(codes.Unimplemented, "Unimplemented")
	}

	return nil
}

func iscsiAddNode(ctx context.Context, iscsi *volumecontext.ISCSI) (err error) {
	if err = execCmd(ctx, "iscsiadm", "-m", "node", "-T", iscsi.Target, "-o", "new", "-p", iscsi.Portal); err != nil {
		return
	}

	if err = execCmd(ctx, "iscsiadm", "-m", "node", "-T", iscsi.Target, "-o", "update", "-n", "node.startup", "-v", "manual"); err != nil {
		return
	}

	if err = execCmd(ctx, "iscsiadm", "-m", "node", "-T", iscsi.Target, "-o", "update", "-n", "node.session.timeo.replacement_timeout", "-v", "-1"); err != nil {
		return
	}

	if iscsi.InboundAuth != nil {
		if err = execCmd(ctx, "iscsiadm", "-m", "node", "-T", iscsi.Target, "-o", "update", "-n", "node.session.auth.authmethod", "-v", "CHAP"); err != nil {
			return
		}

		if err = execCmd(ctx, "iscsiadm", "-m", "node", "-T", iscsi.Target, "-o", "update", "-n", "node.session.auth.username", "-v", iscsi.InboundAuth.Username); err != nil {
			return
		}

		if err = execCmd(ctx, "iscsiadm", "-m", "node", "-T", iscsi.Target, "-o", "update", "-n", "node.session.auth.password", "-v", iscsi.InboundAuth.Password); err != nil {
			return
		}

		if iscsi.OutboundAuth != nil {
			if err = execCmd(ctx, "iscsiadm", "-m", "node", "-T", iscsi.Target, "-o", "update", "-n", "node.session.auth.username_in", "-v", iscsi.OutboundAuth.Username); err != nil {
				return
			}

			if err = execCmd(ctx, "iscsiadm", "-m", "node", "-T", iscsi.Target, "-o", "update", "-n", "node.session.auth.password_in", "-v", iscsi.OutboundAuth.Password); err != nil {
				return
			}
		}
	}

	return nil
}

func iscsiDeleteNode(ctx context.Context, target string) (err error) {
	return execCmd(ctx, "iscsiadm", "-m", "node", "-T", target, "-o", "delete")
}

func iscsiLoginNode(ctx context.Context, target string) error {
	return execCmd(ctx, "iscsiadm", "-m", "node", "-T", target, "-l")
}

func iscsiLogoutNode(ctx context.Context, target string) error {
	return execCmd(ctx, "iscsiadm", "-m", "node", "-T", target, "-u")
}

func iscsiWaitForDevice(ctx context.Context, portal string, target string) (path string, err error) {
	if !strings.Contains(portal, ":") {
		portal = portal + ":3260"
	}
	path = fmt.Sprintf("/dev/disk/by-path/ip-%s-iscsi-%s-lun-0", portal, target)

	tcontext, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if _, err = os.Stat(path); err == nil {
			return
		}

		select {
		case <-tcontext.Done():
			return "", fmt.Errorf("Waiting for device at %s timed out", path)
		case <-ticker.C:
		}
	}
}
