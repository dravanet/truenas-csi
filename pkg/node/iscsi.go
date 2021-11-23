package node

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dravanet/truenas-csi/pkg/csi"
	"github.com/dravanet/truenas-csi/pkg/volumecontext"
)

// Iscsi error codes
const (
	ISCSI_SUCCESS                    = 0
	ISCSI_ERR                        = 1
	ISCSI_ERR_SESS_NOT_FOUND         = 2
	ISCSI_ERR_NOMEM                  = 3
	ISCSI_ERR_TRANS                  = 4
	ISCSI_ERR_LOGIN                  = 5
	ISCSI_ERR_IDBM                   = 6
	ISCSI_ERR_INVAL                  = 7
	ISCSI_ERR_TRANS_TIMEOUT          = 8
	ISCSI_ERR_INTERNAL               = 9
	ISCSI_ERR_LOGOUT                 = 10
	ISCSI_ERR_PDU_TIMEOUT            = 11
	ISCSI_ERR_TRANS_NOT_FOUND        = 12
	ISCSI_ERR_ACCESS                 = 13
	ISCSI_ERR_TRANS_CAPS             = 14
	ISCSI_ERR_SESS_EXISTS            = 15
	ISCSI_ERR_INVALID_MGMT_REQ       = 16
	ISCSI_ERR_ISNS_UNAVAILABLE       = 17
	ISCSI_ERR_ISCSID_COMM_ERR        = 18
	ISCSI_ERR_FATAL_LOGIN            = 19
	ISCSI_ERR_ISCSID_NOTCONN         = 20
	ISCSI_ERR_NO_OBJS_FOUND          = 21
	ISCSI_ERR_SYSFS_LOOKUP           = 22
	ISCSI_ERR_HOST_NOT_FOUND         = 23
	ISCSI_ERR_LOGIN_AUTH_FAILED      = 24
	ISCSI_ERR_ISNS_QUERY             = 25
	ISCSI_ERR_ISNS_REG_FAILED        = 26
	ISCSI_ERR_OP_NOT_SUPP            = 27
	ISCSI_ERR_BUSY                   = 28
	ISCSI_ERR_AGAIN                  = 29
	ISCSI_ERR_UNKNOWN_DISCOVERY_TYPE = 30
	ISCSI_ERR_CHILD_TERMINATED       = 31
	ISCSI_ERR_SESSION_NOT_CONNECTED  = 32
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
}

func iscsiAddNode(ctx context.Context, iscsi *volumecontext.ISCSI) (err error) {
	if err = iscsiadm(ctx, "-m", "node", "-T", iscsi.Target, "-o", "new", "-p", iscsi.Portal); err != nil {
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
		if err = iscsiadm(ctx, "-m", "node", "-T", iscsi.Target, "-o", "update", "-n", setting.Key, "-v", setting.Value); err != nil {
			return
		}
	}

	return nil
}

func iscsiDeleteNode(ctx context.Context, target string) (err error) {
	return passExitCode(iscsiadm(ctx, "-m", "node", "-T", target, "-o", "delete"), ISCSI_ERR_NO_OBJS_FOUND)
}

func iscsiLoginNode(ctx context.Context, target string) (err error) {
	return passExitCode(iscsiadm(ctx, "-m", "node", "-T", target, "-l"), ISCSI_ERR_SESS_EXISTS)
}

func iscsiLogoutNode(ctx context.Context, target string) (err error) {
	return passExitCode(iscsiadm(ctx, "-m", "node", "-T", target, "-u"), ISCSI_ERR_NO_OBJS_FOUND)
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

var iscsiMu sync.Mutex

func iscsiadm(ctx context.Context, args ...string) (err error) {
	// Serialize iscsiadm commands
	iscsiMu.Lock()
	defer iscsiMu.Unlock()

	i := 0

	for {
		if err = execCmd(ctx, "iscsiadm", args...); err == nil {
			return
		}

		// Check exit status for ISCSI_ERR_IDBM
		var exiterror *exec.ExitError
		if !errors.As(err, &exiterror) || exiterror.ExitCode() != ISCSI_ERR_IDBM {
			return
		}

		if i == 2 {
			break
		}

		i++

		time.Sleep(100 * time.Millisecond)
	}

	return
}
