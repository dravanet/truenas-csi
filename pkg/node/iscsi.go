package node

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dravanet/truenas-csi/pkg/csi"
	"github.com/dravanet/truenas-csi/pkg/volumecontext"
)

func (ns *server) stageISCSIVolume(ctx context.Context, req *csi.NodeStageVolumeRequest, iscsi *volumecontext.ISCSI) (err error) {
	portal := iscsi.Portal
	if !strings.Contains(portal, ":") {
		portal = portal + ":3260"
	}

	device := fmt.Sprintf("/dev/disk/by-path/ip-%s-iscsi-%s-lun-0", portal, iscsi.Target)
	_, err = os.Stat(device)

	if err != nil {
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

		if err = iscsiWaitForDevice(ctx, device); err != nil {
			return status.Errorf(codes.Unavailable, "Error waiting for device: %+v", err)
		}
	}

	if err = ioutil.WriteFile(path.Join(req.StagingTargetPath, "iscsi"), []byte(iscsi.Target), 0o640); err != nil {
		return status.Errorf(codes.Unavailable, "Error writing to staging/iscsi: %+v", err)
	}

	devicePath := path.Join(req.StagingTargetPath, "device")
	os.Remove(devicePath)
	if err = os.Symlink(device, devicePath); err != nil {
		return status.Errorf(codes.Unavailable, "Error creating symlink: %+v", err)
	}

	if mount := req.VolumeCapability.GetMount(); mount != nil {
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
					return status.Errorf(codes.Unavailable, "Error creating filesystem: %+v", mkfsErr)
				}
			} else {
				return status.Errorf(codes.Unavailable, "Error determining existing filesystem: %+v", blkidErr)
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
	switch {
	case req.VolumeCapability.GetBlock() != nil:
		device, err := os.Readlink(path.Join(req.StagingTargetPath, "device"))
		if err != nil {
			return status.Errorf(codes.Unavailable, "error reading symlink")
		}
		if oldlink, err := os.Readlink(req.TargetPath); err != nil || device != oldlink {
			os.Remove(req.TargetPath)

			if err = os.Symlink(device, req.TargetPath); err != nil {
				return status.Errorf(codes.Unavailable, "Failed creating symlink at TargetPath")
			}
		}
	case req.VolumeCapability.GetMount() != nil:
		ismnt, _ := isMountPoint(req.TargetPath)
		if !ismnt {
			os.Mkdir(req.TargetPath, 0o755)
			if err := execCmd(ctx, "mount", path.Join(req.StagingTargetPath, "device"), req.TargetPath); err != nil {
				return status.Errorf(codes.Unavailable, "Error mounting filesystem: %+v", err)
			}
		}
	default:
		return status.Errorf(codes.Unimplemented, "Unimplemented")
	}

	return nil
}

func (ns *server) iscsiNodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	if req.StagingTargetPath == "" {
		return nil, status.Errorf(codes.NotFound, "StagingTargetPath not provided")
	}

	device, err := filepath.EvalSymlinks(path.Join(req.StagingTargetPath, "device"))
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "device unaccessible at StagingTargetPath=%s", path.Join(req.StagingTargetPath, "device"))
	}

	if !strings.HasPrefix(device, "/dev/") {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid device read: %s", device)
	}

	blockdevice := strings.TrimPrefix(device, "/dev/")

	rescanPath := path.Join("/sys", "class", "block", blockdevice, "device", "rescan")
	if err = ioutil.WriteFile(rescanPath, []byte("- - -"), 0); err != nil {
		return nil, status.Errorf(codes.Unavailable, "Failed issuing rescan to %s: %+v", rescanPath, err)
	}

	ismnt, _ := isMountPoint(req.VolumePath)
	if ismnt {
		var statfs unix.Statfs_t
		if err := unix.Statfs(req.VolumePath, &statfs); err != nil {
			return nil, status.Errorf(codes.Unavailable, "statfs failed on %q", req.VolumePath)
		}

		switch statfs.Type {
		// case unix.EXT2_SUPER_MAGIC, unix.EXT3_SUPER_MAGIC, unix.EXT4_SUPER_MAGIC:
		case unix.EXT4_SUPER_MAGIC:
			err = execCmd(ctx, "resize2fs", device)
		case unix.XFS_SUPER_MAGIC:
			err = execCmd(ctx, "xfs_growfs", req.VolumePath)
		}

		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "growing filesystem at %q (device: %q) failed", req.VolumePath, device)
		}
	}

	return &csi.NodeExpandVolumeResponse{}, nil
}

// nodeSetting holds a setting key/value for an iscsi node
type nodeSetting struct {
	Key   string
	Value string
}

var defaultNodeSettings = []nodeSetting{
	{"node.startup", "manual"},
	{"node.session.timeo.replacement_timeout", "-1"},
}

func iscsiAddNode(ctx context.Context, iscsi *volumecontext.ISCSI) (err error) {
	if err = execCmd(ctx, "iscsiadm", "-m", "node", "-T", iscsi.Target, "-o", "new", "-p", iscsi.Portal); err != nil {
		return
	}

	settings := defaultNodeSettings
	if iscsi.InboundAuth != nil {
		settings = append(settings,
			nodeSetting{"node.session.auth.authmethod", "CHAP"},
			nodeSetting{"node.session.auth.username", iscsi.InboundAuth.Username},
			nodeSetting{"node.session.auth.password", iscsi.InboundAuth.Password},
		)

		if iscsi.OutboundAuth != nil {
			settings = append(settings,
				nodeSetting{"node.session.auth.username_in", iscsi.OutboundAuth.Username},
				nodeSetting{"node.session.auth.password_in", iscsi.OutboundAuth.Password},
			)
		}
	}

	for _, setting := range settings {
		if err = execCmd(ctx, "iscsiadm", "-m", "node", "-T", iscsi.Target, "-o", "update", "-n", setting.Key, "-v", setting.Value); err != nil {
			return
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

func iscsiWaitForDevice(ctx context.Context, device string) (err error) {
	tcontext, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if _, err = os.Stat(device); err == nil {
			return
		}

		select {
		case <-tcontext.Done():
			return fmt.Errorf("Waiting for device at %s timed out", device)
		case <-ticker.C:
		}
	}
}
