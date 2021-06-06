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
	TruenasOapi "github.com/dravanet/truenas-csi/pkg/truenas"
	"github.com/dravanet/truenas-csi/pkg/volumecontext"
	"github.com/google/uuid"
)

func (cs *server) createISCSIVolume(ctx context.Context, req *csi.CreateVolumeRequest, nas *config.FreeNAS) (
	dataset string,
	capacityBytes int64,
	volumeContext *volumecontext.VolumeContext,
	err error) {

	cl, err := newTruenasOapiClient(nas)
	if err != nil {
		err = status.Error(codes.Unavailable, "creating FreenasOapi client failed")
		return
	}

	iscsi := cs.findISCSIconfiguration(req, nas)
	if iscsi == nil {
		err = status.Error(codes.InvalidArgument, "could not find a suitable iscsi configuration")
		return
	}

	var extentID int
	var targetName string
	var targetID int = -1
	var iscsiUsername string
	var iscsiSecret string

	capacityBytes = req.CapacityRange.GetLimitBytes()
	if capacityBytes == 0 {
		capacityBytes = req.CapacityRange.GetRequiredBytes()
	}

	extent, err := cs.iscsiGetextentByComment(ctx, cl, req.Name)
	if err != nil {
		return
	}

	if extent == nil {
		// Create the dataset + extent
		targetName = uuid.NewString()
		dataset = path.Join(iscsi.RootDataset, targetName)

		// Create ZVOL
		voltype := TruenasOapi.PoolDatasetCreate0TypeVOLUME
		// set comment temporarily
		comment := req.Name

		volsize := int(capacityBytes)
		if _, err = handleNasResponse(cl.PostPoolDataset(ctx, TruenasOapi.PostPoolDatasetJSONRequestBody{
			Name:     &dataset,
			Type:     &voltype,
			Volsize:  &volsize,
			Sparse:   &iscsi.Sparse,
			Comments: &comment,
		})); err != nil {
			return
		}

		defer func() {
			if err != nil {
				recursive := true
				cl.DeletePoolDatasetIdId(ctx, dataset, TruenasOapi.DeletePoolDatasetIdIdJSONRequestBody{
					Recursive: &recursive,
				})
			}
		}()

		// Create ISCSI Extent
		extenttype := TruenasOapi.IscsiExtentCreate0TypeDISK
		zvolpath := path.Join("zvol", dataset)
		insercuretpc := false
		sbytes := make([]byte, 7)
		rand.Read(sbytes)
		serial := hex.EncodeToString(sbytes)

		if extentID, err = handleNasCreateResponse(cl.PostIscsiExtent(ctx, TruenasOapi.PostIscsiExtentJSONRequestBody{
			Name:        &targetName,
			Type:        &extenttype,
			Disk:        &zvolpath,
			InsecureTpc: &insercuretpc,
			Comment:     &req.Name,
			Serial:      &serial,
		})); err != nil {
			return
		}
	} else {
		extentID = extent.ID
		dataset = strings.TrimPrefix(extent.Disk, "zvol/")

		var ds *datasetInfo
		ds, err = cs.getDataset(ctx, cl, dataset)
		if err != nil {
			return
		}

		if ds == nil {
			err = status.Error(codes.Internal, "Dataset not found")
			return
		}

		if ds.Volsize != nil {
			if capacityBytes != *ds.Volsize {
				err = status.Error(codes.AlreadyExists, "Creating existing volume with different capacity")
				return
			}
		}

		var target *iscsiTarget
		target, err = cs.iscsiGetTargetByExtent(ctx, cl, extentID)
		if err != nil {
			return
		}

		if target != nil {
			targetName = *target.Name
			targetID = target.ID

			var auth *iscsiAuth
			auth, err = cs.getIscsiAuth(ctx, cl, target)
			if err != nil {
				return
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
	if _, err = handleNasResponse(cl.PutPoolDatasetIdId(ctx, dataset, TruenasOapi.PutPoolDatasetIdIdJSONRequestBody{
		Comments: &comments,
	})); err != nil {
		return
	}

	// targetName already populated

	if targetID == -1 {
		// Create auth
		iscsiUsername = targetName
		iscsiSecret = genIscsiSecret()
		tag := int(-1)

		if tag, err = handleNasCreateResponse(cl.PostIscsiAuth(ctx, TruenasOapi.PostIscsiAuthJSONRequestBody{
			Tag:    &tag,
			User:   &iscsiUsername,
			Secret: &iscsiSecret,
		})); err != nil {
			return
		}

		defer func() {
			if err != nil {
				cl.DeleteIscsiAuthIdId(ctx, tag)
			}
		}()

		// Update auth group
		if _, err = handleNasResponse(cl.PutIscsiAuthIdId(ctx, tag, TruenasOapi.PutIscsiAuthIdIdJSONRequestBody{
			Tag: &tag,
		})); err != nil {
			return
		}

		// Create target
		if targetID, err = handleNasCreateResponse(cl.PostIscsiTarget(ctx, TruenasOapi.PostIscsiTargetJSONRequestBody{
			Name: &targetName,
			Groups: &[]map[string]interface{}{
				{
					"portal":     iscsi.PortalID,
					"authmethod": "CHAP",
					"auth":       tag,
				},
			},
		})); err != nil {
			return
		}

		defer func() {
			if err != nil {
				cl.DeleteIscsiTargetIdId(ctx, targetID, TruenasOapi.DeleteIscsiTargetIdIdJSONRequestBody(false))
			}
		}()

		// Create association
		lunid := 0
		if _, err = handleNasResponse(cl.PostIscsiTargetextent(ctx, TruenasOapi.PostIscsiTargetextentJSONRequestBody{
			Target: &targetID,
			Extent: &extentID,
			Lunid:  &lunid,
		})); err != nil {
			return
		}
	}

	// Obtain Target name
	var iscsiglobalresp []byte
	if iscsiglobalresp, err = handleNasResponse(cl.GetIscsiGlobal(ctx)); err != nil {
		return
	}
	var result TruenasOapi.PutIscsiGlobalJSONBody
	if err = json.Unmarshal(iscsiglobalresp, &result); err != nil {
		return
	}
	if result.Basename == nil {
		err = status.Errorf(codes.Unavailable, "Error parsing freenas response: missing basename")
		return
	}

	volumeContext = &volumecontext.VolumeContext{
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

	return
}

func (cs *server) deleteISCSIVolume(ctx context.Context, cl *TruenasOapi.Client, di *datasetInfo) error {
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
	if _, err := cl.DeleteIscsiExtentIdId(ctx, int(extent), TruenasOapi.DeleteIscsiExtentIdIdJSONRequestBody{}); err != nil {
		return status.Errorf(codes.Unavailable, "Error during call to Nas: %+v", err)
	}

	return nil
}

type iscsiExtent struct {
	ID      int    `json:"id"`
	Comment string `json:"comment"`
	Disk    string `json:"disk"`
}

func (cs *server) iscsiGetextentByComment(ctx context.Context, cl *TruenasOapi.Client, comment string) (ret *iscsiExtent, err error) {
	var extentresp []byte
	if extentresp, err = handleNasResponse(cl.GetIscsiExtent(ctx, &TruenasOapi.GetIscsiExtentParams{}, truenasOapiFilter("comment", comment))); err != nil {
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

func (cs *server) iscsiGetTargetByExtent(ctx context.Context, cl *TruenasOapi.Client, extent int) (ret *iscsiTarget, err error) {
	var targetextentbody []byte
	if targetextentbody, err = handleNasResponse(cl.GetIscsiTargetextent(ctx, &TruenasOapi.GetIscsiTargetextentParams{}, truenasOapiFilter("extent", fmt.Sprintf("%d", extent)))); err != nil {
		return
	}
	var extents []struct {
		Extent int `json:"extent"`
		Target int `json:"target"`
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
		if extents[0].Extent != extent {
			err = status.Errorf(codes.Unavailable, "Unexpected result from NAS, does filtering work?: %+v", extents)

			return
		}

		var targetbody []byte
		if targetbody, err = handleNasResponse(cl.GetIscsiTargetIdId(ctx, extents[0].Target, &TruenasOapi.GetIscsiTargetIdIdParams{})); err != nil {
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
	TruenasOapi.PostIscsiAuthJSONBody
}

func (cs *server) getIscsiAuth(ctx context.Context, cl *TruenasOapi.Client, target *iscsiTarget) (ret *iscsiAuth, err error) {
	if len(target.Groups) > 1 {
		err = status.Errorf(codes.Unavailable, "Unexpected result: too many groups assigned: %+v", target)

		return
	}

	if len(target.Groups) == 0 || target.Groups[0].Auth == nil {
		return
	}

	var authresp []byte
	if authresp, err = handleNasResponse(cl.GetIscsiAuth(ctx, &TruenasOapi.GetIscsiAuthParams{}, truenasOapiFilter("tag", fmt.Sprintf("%d", *target.Groups[0].Auth)))); err != nil {
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

func (cs *server) findISCSIconfiguration(req *csi.CreateVolumeRequest, nas *config.FreeNAS) *config.ISCSI {
	configName := req.GetParameters()[configSelector]
	if configName == "" {
		configName = "default"
	}

	return nas.ISCSI[configName]
}

func genIscsiSecret() string {
	password := make([]byte, 12)
	rand.Read(password)

	return base64.StdEncoding.EncodeToString(password)
}
