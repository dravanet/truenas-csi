package controller

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"

	"github.com/dravanet/truenas-csi/pkg/config"
	"github.com/dravanet/truenas-csi/pkg/csi"
	FreenasOapi "github.com/dravanet/truenas-csi/pkg/freenas"
	"github.com/dravanet/truenas-csi/pkg/volumecontext"
	"github.com/google/uuid"
)

func (cs *server) createISCSIVolume(ctx context.Context, req *csi.CreateVolumeRequest) (resp *csi.CreateVolumeResponse, err error) {
	cl, err := newFreenasOapiClient(cs.freenas)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "creating FreenasOapi client failed")
	}

	iscsi := cs.findISCSIconfiguration(req)
	if iscsi == nil {
		return nil, status.Error(codes.InvalidArgument, "could not find a suitable iscsi configuration")
	}

	var dataset string
	var extentID int
	var targetName string
	var targetID int = -1
	var iscsiUsername string
	var iscsiSecret string

	capacityBytes := req.CapacityRange.GetLimitBytes()
	if capacityBytes == 0 {
		capacityBytes = req.CapacityRange.GetRequiredBytes()
	}

	extent, err := cs.iscsiGetextentByComment(ctx, cl, req.Name)
	if err != nil {
		return nil, err
	}

	if extent == nil {
		// Create the dataset + extent
		targetName = uuid.NewString()
		dataset = path.Join(iscsi.RootDataset, targetName)

		// Create ZVOL
		voltype := "VOLUME"
		// set comment temporarily
		comment := req.Name

		volsize := int(capacityBytes)
		if _, err := handleNasResponse(cl.PostPoolDataset(ctx, FreenasOapi.PostPoolDatasetJSONRequestBody{
			Name:     &dataset,
			Type:     &voltype,
			Volsize:  &volsize,
			Sparse:   &iscsi.Sparse,
			Comments: &comment,
		})); err != nil {
			return nil, err
		}

		defer func() {
			if err != nil {
				recursive := true
				cl.DeletePoolDatasetIdId(ctx, dataset, FreenasOapi.DeletePoolDatasetIdIdJSONRequestBody{
					Recursive: &recursive,
				})
			}
		}()

		// Create ISCSI Extent
		extenttype := "DISK"
		zvolpath := path.Join("zvol", dataset)
		insercuretpc := false
		sbytes := make([]byte, 7)
		rand.Read(sbytes)
		serial := hex.EncodeToString(sbytes)

		if extentID, err = handleNasCreateResponse(cl.PostIscsiExtent(ctx, FreenasOapi.PostIscsiExtentJSONRequestBody{
			Name:        &targetName,
			Type:        &extenttype,
			Disk:        &zvolpath,
			InsecureTpc: &insercuretpc,
			Comment:     &req.Name,
			Serial:      &serial,
		})); err != nil {
			return nil, err
		}
	} else {
		extentID = extent.ID
		dataset = strings.TrimPrefix(extent.Disk, "zvol/")

		ds, err := cs.getDataset(ctx, cl, dataset)
		if err != nil {
			return nil, err
		}

		if ds == nil {
			return nil, status.Error(codes.Internal, "Dataset not found")
		}

		if ds.Volsize != nil {
			if capacityBytes != *ds.Volsize {
				return nil, status.Error(codes.AlreadyExists, "Creating existing volume with different capacity")
			}
		}

		target, err := cs.iscsiGetTargetByExtent(ctx, cl, extentID)
		if err != nil {
			return nil, err
		}

		if target != nil {
			targetName = *target.Name
			targetID = target.ID

			auth, err := cs.getIscsiAuth(ctx, cl, target)
			if err != nil {
				return nil, err
			}

			if auth != nil {
				iscsiUsername = *auth.User
				iscsiSecret = *auth.Secret
			}
		} else {
			_, targetName = path.Split(dataset)
		}
	}

	// ensure comment is set on dataset, in case of an interrupted create process
	comments := fmt.Sprintf("extent:%d", extentID)
	if _, err = handleNasResponse(cl.PutPoolDatasetIdId(ctx, dataset, FreenasOapi.PutPoolDatasetIdIdJSONRequestBody{
		Comments: &comments,
	})); err != nil {
		return nil, err
	}

	// targetName already populated

	if targetID == -1 {
		// Create auth
		iscsiUsername = targetName
		iscsiSecret = genIscsiSecret()
		tag := int(-1)

		if tag, err = handleNasCreateResponse(cl.PostIscsiAuth(ctx, FreenasOapi.PostIscsiAuthJSONRequestBody{
			Tag:    &tag,
			User:   &iscsiUsername,
			Secret: &iscsiSecret,
		})); err != nil {
			return nil, err
		}

		defer func() {
			if err != nil {
				cl.DeleteIscsiAuthIdId(ctx, tag)
			}
		}()

		// Update auth group
		if _, err := handleNasResponse(cl.PutIscsiAuthIdId(ctx, tag, FreenasOapi.PutIscsiAuthIdIdJSONRequestBody{
			Tag: &tag,
		})); err != nil {
			return nil, err
		}

		// Create target
		if targetID, err = handleNasCreateResponse(cl.PostIscsiTarget(ctx, FreenasOapi.PostIscsiTargetJSONRequestBody{
			Name: &targetName,
			Groups: &[]map[string]interface{}{
				{
					"portal":     iscsi.PortalID,
					"authmethod": "CHAP",
					"auth":       tag,
				},
			},
		})); err != nil {
			return nil, err
		}

		defer func() {
			if err != nil {
				cl.DeleteIscsiTargetIdId(ctx, targetID, FreenasOapi.DeleteIscsiTargetIdIdJSONRequestBody(false))
			}
		}()

		// Create association
		lunid := 0
		if _, err := handleNasResponse(cl.PostIscsiTargetextent(ctx, FreenasOapi.PostIscsiTargetextentJSONRequestBody{
			Target: &targetID,
			Extent: &extentID,
			Lunid:  &lunid,
		})); err != nil {
			return nil, err
		}
	}

	// Obtain Target name
	var iscsiglobalresp []byte
	if iscsiglobalresp, err = handleNasResponse(cl.GetIscsiGlobal(ctx)); err != nil {
		return nil, err
	}
	var result FreenasOapi.PutIscsiGlobalJSONBody
	if err = json.Unmarshal(iscsiglobalresp, &result); err != nil {
		return nil, err
	}
	if result.Basename == nil {
		return nil, status.Errorf(codes.Unavailable, "Error parsing freenas response: missing basename")
	}

	volumeContext := &volumecontext.VolumeContext{
		Iscsi: &volumecontext.ISCSI{
			Portal: iscsi.Portal,
			Target: fmt.Sprintf("%s:%s", *result.Basename, targetName),
		},
	}

	if iscsiUsername != "" {
		volumeContext.Iscsi.InboundAuth = &volumecontext.ISCSIAuth{
			Username: iscsiUsername,
			Password: iscsiSecret,
		}
	}

	serialized, _ := volumecontext.Base64Serializer().Serialize(volumeContext)

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			CapacityBytes: capacityBytes,
			VolumeId:      dataset,
			VolumeContext: map[string]string{
				"b64": serialized,
			},
		},
	}, nil
}

func (cs *server) deleteISCSIVolume(ctx context.Context, cl *FreenasOapi.Client, di *datasetInfo) error {
	extent, err := strconv.ParseInt(strings.TrimPrefix(di.Comments, "extent:"), 10, 32)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "Failed parsing extentID: %+v", err)
	}

	// Find target
	target, err := cs.iscsiGetTargetByExtent(ctx, cl, int(extent))
	if err != nil {
		return err
	}

	if target != nil {
		// Lookup auth
		auth, err := cs.getIscsiAuth(ctx, cl, target)
		if err != nil {
			return err
		}

		// Delete target
		if _, err = handleNasResponse(cl.DeleteIscsiTargetIdId(ctx, target.ID, false)); err != nil {
			return err
		}

		// Delete auth
		if auth != nil {
			if _, err = handleNasResponse(cl.DeleteIscsiAuthIdId(ctx, auth.ID)); err != nil {
				return err
			}
		}
	}

	// Delete extent
	if _, err := cl.DeleteIscsiExtentIdId(ctx, int(extent), FreenasOapi.DeleteIscsiExtentIdIdJSONRequestBody{}); err != nil {
		return status.Errorf(codes.Unavailable, "Error during call to Nas: %+v", err)
	}

	return nil
}

type iscsiExtent struct {
	ID      int    `json:"id"`
	Comment string `json:"comment"`
	Disk    string `json:"disk"`
}

func (cs *server) iscsiGetextentByComment(ctx context.Context, cl *FreenasOapi.Client, comment string) (ret *iscsiExtent, err error) {
	var extentresp []byte
	if extentresp, err = handleNasResponse(cl.GetIscsiExtent(ctx, &FreenasOapi.GetIscsiExtentParams{}, freenasOapiFilter("comment", comment))); err != nil {
		return nil, err
	}

	var extents []iscsiExtent
	if err = json.Unmarshal(extentresp, &extents); err != nil {
		return nil, status.Errorf(codes.Unavailable, "Error parsing result from NAS: %+v", err)
	}
	if len(extents) > 1 {
		return nil, status.Errorf(codes.Unavailable, "Unexpected result from NAS: %+v", extents)
	}
	if len(extents) == 1 {
		if extents[0].Comment != comment {
			return nil, status.Errorf(codes.Unavailable, "Unexpected result from NAS: %+v", extents)
		}

		ret = &extents[0]
	}

	return
}

type iscsiTarget struct {
	ID int `json:"id"`

	Groups []struct {
		Auth       *int   `json:"auth"`
		Authmethod string `json:"authmethod"`
	} `json:"groups,omitempty"`
	Mode *string `json:"mode,omitempty"`
	Name *string `json:"name,omitempty"`
}

func (cs *server) iscsiGetTargetByExtent(ctx context.Context, cl *FreenasOapi.Client, extent int) (ret *iscsiTarget, err error) {
	var targetextentbody []byte
	if targetextentbody, err = handleNasResponse(cl.GetIscsiTargetextent(ctx, &FreenasOapi.GetIscsiTargetextentParams{}, freenasOapiFilter("extent", fmt.Sprintf("%d", extent)))); err != nil {
		return
	}
	var extents []struct {
		Extent *int `json:"extent"`
		Target *int `json:"target"`
	}
	if err = json.Unmarshal(targetextentbody, &extents); err != nil {
		err = status.Errorf(codes.Unavailable, "Error parsing result from NAS: %+v", err)

		return
	}
	if len(extents) > 1 {
		err = status.Errorf(codes.Unavailable, "Unexpected result from NAS: %+v", extents)

		return
	}
	if len(extents) == 1 {
		if *extents[0].Extent != extent {
			err = status.Errorf(codes.Unavailable, "Unexpected result from NAS, does filtering work?: %+v", extents)

			return
		}

		var targetbody []byte
		if targetbody, err = handleNasResponse(cl.GetIscsiTargetIdId(ctx, []interface{}{*extents[0].Target}, &FreenasOapi.GetIscsiTargetIdIdParams{})); err != nil {
			return
		}

		var target iscsiTarget
		if err = json.Unmarshal(targetbody, &target); err != nil {
			err = status.Errorf(codes.Unavailable, "Failed parsing NAS response: %+v", err)

			return
		}

		ret = &target
	}

	return
}

type iscsiAuth struct {
	ID int `json:"id"`
	FreenasOapi.PostIscsiAuthJSONBody
}

func (cs *server) getIscsiAuth(ctx context.Context, cl *FreenasOapi.Client, target *iscsiTarget) (ret *iscsiAuth, err error) {
	if len(target.Groups) > 1 {
		err = status.Errorf(codes.Unavailable, "Unexpected result: too many groups assigned: %+v", target)

		return
	}

	if len(target.Groups) == 0 || target.Groups[0].Auth == nil {
		return
	}

	var authresp []byte
	if authresp, err = handleNasResponse(cl.GetIscsiAuth(ctx, &FreenasOapi.GetIscsiAuthParams{}, freenasOapiFilter("tag", fmt.Sprintf("%d", *target.Groups[0].Auth)))); err != nil {
		return
	}

	var auths []iscsiAuth
	if err = json.Unmarshal(authresp, &auths); err != nil {
		err = status.Errorf(codes.Unavailable, "Unexpected result: too many groups assigned: %+v", target)

		return
	}

	if len(auths) > 1 {
		err = status.Errorf(codes.Unavailable, "Unexpected result: too many auth found: %+v", auths)

		return
	}

	if len(auths) == 1 {
		if auths[0].Tag == nil || *auths[0].Tag != *target.Groups[0].Auth {
			err = status.Errorf(codes.Unavailable, "Unexpected result: invalid auth received: %+v", auths)

			return
		}

		ret = &auths[0]
	}

	return
}

func (cs *server) findISCSIconfiguration(req *csi.CreateVolumeRequest) *config.ISCSI {
	for _, iscsi := range cs.freenas.ISCSI {
		return iscsi
	}

	return nil
}

func genIscsiSecret() string {
	password := make([]byte, 12)
	rand.Read(password)

	return base64.StdEncoding.EncodeToString(password)
}
