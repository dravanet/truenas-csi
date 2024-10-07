package controller

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strings"

	"google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"

	"github.com/dravanet/truenas-csi/pkg/config"
	TruenasOapi "github.com/dravanet/truenas-csi/pkg/truenas"
	"github.com/dravanet/truenas-csi/pkg/volumecontext"
)

func (cs *server) createISCSIVolume(ctx context.Context, cl *TruenasOapi.Client, iscsi *config.ISCSI, reqName string, dataset string, targetName string) (
	volumeContext *volumecontext.VolumeContext,
	err error) {

	var targetID int
	var extentID int
	var iscsiUsername string
	var iscsiSecret string

	// Create ISCSI Extent
	extenttype := TruenasOapi.IscsiExtentCreate0TypeDISK
	zvolpath := path.Join("zvol", dataset)
	insercuretpc := false
	sbytes := make([]byte, 7)
	rand.Read(sbytes)
	serial := hex.EncodeToString(sbytes)

	extentcreateresp, err := cl.PostIscsiExtent(ctx, TruenasOapi.PostIscsiExtentJSONRequestBody{
		Name:        &targetName,
		Type:        &extenttype,
		Disk:        &zvolpath,
		InsecureTpc: &insercuretpc,
		Comment:     &reqName,
		Serial:      &serial,
		Pblocksize:  &iscsi.DisableReportBlockSize,
	})
	if err != nil {
		return
	}
	switch extentcreateresp.StatusCode {
	case 200:
		// Create succeeded
		extentID, err = handleNasCreateResponse(extentcreateresp, nil)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "failed parsing result")
		}
	default:
		// Create failed due to conflict or other errors
		var extent *iscsiExtent
		extent, err = cs.getISCSIExtentByName(ctx, cl, targetName)
		if err != nil {
			return
		}

		if extent == nil {
			return nil, status.Errorf(codes.Unavailable, "failed querying existing extent %q: not found", targetName)
		}

		extentDataset := strings.TrimPrefix(extent.Disk, "zvol/")
		if extentDataset != dataset {
			return nil, fmt.Errorf("extent %q uses dataset %q (expected: %q)", targetName, extentDataset, dataset)
		}

		extentID = extent.ID
	}

	// Create iSCSI
	var target *iscsiTarget
	target, err = cs.getISCSITargetByName(ctx, cl, targetName)
	if err != nil {
		return
	}

	if target == nil {
		// Lookup existing auth
		var auth *iscsiAuth
		auth, err = cs.getIscsiAuthByUser(ctx, cl, targetName)
		if err != nil {
			return
		}

		if auth == nil {
			// Create auth
			iscsiUsername = targetName
			iscsiSecret = genIscsiSecret()
			tag := int(-1)

			auth = &iscsiAuth{
				ID: tag,
				PostIscsiAuthJSONRequestBody: TruenasOapi.PostIscsiAuthJSONRequestBody{
					Tag:    &tag,
					User:   &iscsiUsername,
					Secret: &iscsiSecret,
				},
			}

			if auth.ID, err = handleNasCreateResponse(cl.PostIscsiAuth(ctx, auth.PostIscsiAuthJSONRequestBody)); err != nil {
				return
			}
		} else {
			iscsiUsername = *auth.User
			iscsiSecret = *auth.Secret
		}

		// Update auth group
		if *auth.Tag == -1 {
			if _, err = handleNasResponse(cl.PutIscsiAuthIdId(ctx, auth.ID, TruenasOapi.PutIscsiAuthIdIdJSONRequestBody{
				Tag: &auth.ID,
			})); err != nil {
				return
			}
		}

		// Create target
		if targetID, err = handleNasCreateResponse(cl.PostIscsiTarget(ctx, TruenasOapi.PostIscsiTargetJSONRequestBody{
			Name: &targetName,
			Groups: &[]map[string]interface{}{
				{
					"portal":     iscsi.PortalID,
					"authmethod": "CHAP",
					"auth":       auth.ID,
				},
			},
		})); err != nil {
			return
		}
	} else {
		var auth *iscsiAuth
		auth, err = cs.getIscsiAuthByTarget(ctx, cl, target)
		if err != nil {
			return
		}

		if auth != nil {
			iscsiUsername = *auth.User
			iscsiSecret = *auth.Secret
		}
	}

	// Create association
	lunid := 0
	assoccreateresponse, err := cl.PostIscsiTargetextent(ctx, TruenasOapi.PostIscsiTargetextentJSONRequestBody{
		Target: &targetID,
		Extent: &extentID,
		Lunid:  &lunid,
	})
	if err != nil {
		return
	}
	switch assoccreateresponse.StatusCode {
	case 200:
	default:
		// Create failed due to conflict or other errors
		var assoc *http.Response
		assoc, err = cl.GetIscsiTargetextent(ctx, &TruenasOapi.GetIscsiTargetextentParams{},
			truenasOapiFilter("target", fmt.Sprintf("%d", targetID)),
			truenasOapiFilter("extent", fmt.Sprintf("%d", extentID)),
		)
		if err != nil {
			return
		}
		if assoc.StatusCode != 200 {
			return nil, status.Errorf(codes.Unavailable, "failed querying existing targetextent (%d, %d): not found", targetID, extentID)
		}
	}

	// Obtain Target name
	var iscsiglobalresp []byte
	if iscsiglobalresp, err = handleNasResponse(cl.GetIscsiGlobal(ctx)); err != nil {
		return
	}
	var result TruenasOapi.IscsiGlobalUpdate0
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
	_, targetName := path.Split(di.ID)

	// Lookup target
	target, err := cs.getISCSITargetByName(ctx, cl, targetName)
	if err != nil {
		return err
	}

	if target != nil {
		// Lookup auth
		auth, err := cs.getIscsiAuthByTarget(ctx, cl, target)
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

	// Lookup extent
	extent, err := cs.getISCSIExtentByName(ctx, cl, targetName)
	if err != nil {
		return err
	}

	if extent != nil {
		// Delete extent
		if _, err := cl.DeleteIscsiExtentIdId(ctx, extent.ID, TruenasOapi.DeleteIscsiExtentIdIdJSONRequestBody{}); err != nil {
			return status.Errorf(codes.Unavailable, "Error during call to Nas: %+v", err)
		}
	}

	return nil
}

type iscsiExtent struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Disk string `json:"disk"`
}

func (cs *server) getISCSIExtentByName(ctx context.Context, cl *TruenasOapi.Client, name string) (ret *iscsiExtent, err error) {
	var extentresp []byte
	if extentresp, err = handleNasResponse(cl.GetIscsiExtent(ctx, &TruenasOapi.GetIscsiExtentParams{}, truenasOapiFilter("name", name))); err != nil {
		return
	}

	var extents []iscsiExtent
	if err = json.Unmarshal(extentresp, &extents); err != nil {
		return nil, status.Errorf(codes.Unavailable, "Error parsing result from NAS: %+v", err)
	}
	if len(extents) > 1 {
		return nil, status.Errorf(codes.Unavailable, "Unexpected result from NAS: %+v", extents)
	}
	if len(extents) == 1 {
		if extents[0].Name != name {
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

func (cs *server) getISCSITargetByName(ctx context.Context, cl *TruenasOapi.Client, name string) (ret *iscsiTarget, err error) {
	var targetresp []byte
	if targetresp, err = handleNasResponse(cl.GetIscsiTarget(ctx, &TruenasOapi.GetIscsiTargetParams{}, truenasOapiFilter("name", name))); err != nil {
		return
	}

	var targets []iscsiTarget
	if err = json.Unmarshal(targetresp, &targets); err != nil {
		return nil, status.Errorf(codes.Unavailable, "Error parsing result from NAS: %+v", err)
	}

	if len(targets) > 1 {
		return nil, status.Errorf(codes.Unavailable, "Unexpected result from NAS: %+v", targets)
	}
	if len(targets) == 1 {
		if targets[0].Name == nil || *targets[0].Name != name {
			return nil, status.Errorf(codes.Unavailable, "Unexpected result from NAS: %+v", targets)
		}

		ret = &targets[0]
	}

	return
}

type iscsiAuth struct {
	ID int `json:"id"`
	TruenasOapi.PostIscsiAuthJSONRequestBody
}

func (cs *server) getIscsiAuthByTarget(ctx context.Context, cl *TruenasOapi.Client, target *iscsiTarget) (ret *iscsiAuth, err error) {
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
		return nil, status.Errorf(codes.Unavailable, "Error parsing result from NAS: %+v", err)
	}

	if len(auths) > 1 {
		return nil, status.Errorf(codes.Unavailable, "Unexpected result: too many auth found: %+v", auths)
	}

	if len(auths) == 1 {
		if auths[0].Tag == nil || *auths[0].Tag != *target.Groups[0].Auth {
			return nil, status.Errorf(codes.Unavailable, "Unexpected result: invalid auth received: %+v", auths)
		}

		ret = &auths[0]
	}

	return
}

func (cs *server) getIscsiAuthByUser(ctx context.Context, cl *TruenasOapi.Client, user string) (ret *iscsiAuth, err error) {
	var authresp []byte
	if authresp, err = handleNasResponse(cl.GetIscsiAuth(ctx, &TruenasOapi.GetIscsiAuthParams{}, truenasOapiFilter("user", user))); err != nil {
		return
	}

	var auths []iscsiAuth
	if err = json.Unmarshal(authresp, &auths); err != nil {
		return nil, status.Errorf(codes.Unavailable, "Error parsing result from NAS: %+v", err)
	}

	if len(auths) > 1 {
		return nil, status.Errorf(codes.Unavailable, "Unexpected result: too many auth found: %+v", auths)
	}

	if len(auths) == 1 {
		if auths[0].User == nil || *auths[0].User != user {
			return nil, status.Errorf(codes.Unavailable, "Unexpected result: invalid auth received: %+v", auths)
		}

		ret = &auths[0]
	}

	return
}

func genIscsiSecret() string {
	password := make([]byte, 12)
	rand.Read(password)

	return base64.StdEncoding.EncodeToString(password)
}
