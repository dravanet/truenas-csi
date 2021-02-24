package controller

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"

	"github.com/dravanet/truenas-csi/pkg/config"
	"github.com/dravanet/truenas-csi/pkg/csi"
	FreenasOapi "github.com/dravanet/truenas-csi/pkg/freenas"
	"github.com/dravanet/truenas-csi/pkg/volumecontext"
)

func (cs *server) createISCSIVolume(ctx context.Context, req *csi.CreateVolumeRequest) (resp *csi.CreateVolumeResponse, err error) {
	iscsi := cs.findISCSIconfiguration(req)

	if iscsi == nil {
		return nil, status.Error(codes.InvalidArgument, "could not find a suitable iscsi configuration")
	}

	cl, err := newFreenasOapiClient(cs.freenas)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "creating FreenasOapi client failed")
	}

	// TODO: query for existing portal with req.Name

	// Create auth
	user := req.Name
	secret := genIscsiSecret()
	tag := int(-1)

	if tag, err = handleNasCreateResponse(cl.PostIscsiAuth(ctx, FreenasOapi.PostIscsiAuthJSONRequestBody{
		Tag:    &tag,
		User:   &user,
		Secret: &secret,
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
	var targetID int
	if targetID, err = handleNasCreateResponse(cl.PostIscsiTarget(ctx, FreenasOapi.PostIscsiTargetJSONRequestBody{
		Name: &req.Name,
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

	volsize := int(req.CapacityRange.GetLimitBytes())
	if volsize == 0 {
		volsize = int(req.CapacityRange.GetRequiredBytes())
	}

	dataset := path.Join(iscsi.RootDataset, req.Name)

	// Create ZVOL
	voltype := "VOLUME"
	if _, err := handleNasResponse(cl.PostPoolDataset(ctx, FreenasOapi.PostPoolDatasetJSONRequestBody{
		Name:     &dataset,
		Type:     &voltype,
		Volsize:  &volsize,
		Comments: &req.Name,
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

	var extentID int
	if extentID, err = handleNasCreateResponse(cl.PostIscsiExtent(ctx, FreenasOapi.PostIscsiExtentJSONRequestBody{
		Name:        &req.Name,
		Type:        &extenttype,
		Disk:        &zvolpath,
		InsecureTpc: &insercuretpc,
		Comment:     &req.Name,
	})); err != nil {
		return nil, err
	}

	lunid := 0
	if _, err := handleNasResponse(cl.PostIscsiTargetextent(ctx, FreenasOapi.PostIscsiTargetextentJSONRequestBody{
		Target: &targetID,
		Extent: &extentID,
		Lunid:  &lunid,
	})); err != nil {
		return nil, err
	}

	// Obtain Target name
	var iscsiglobalresp []byte
	if iscsiglobalresp, err = handleNasResponse(cl.GetIscsiGlobal(ctx)); err != nil {
		return nil, err
	}
	var result struct {
		Basename *string `json:"basename"`
	}
	if err = json.Unmarshal(iscsiglobalresp, &result); err != nil {
		return nil, err
	}
	if result.Basename == nil {
		return nil, status.Errorf(codes.Unavailable, "Error parsing freenas response: missing basename")
	}

	volumeContext := &volumecontext.VolumeContext{
		Iscsi: &volumecontext.ISCSI{
			Portal: iscsi.Portal,
			Target: fmt.Sprintf("%s:%s", *result.Basename, req.Name),
			InboundAuth: &volumecontext.ISCSIAuth{
				Username: user,
				Password: secret,
			},
		},
	}

	serialized, _ := volumecontext.Base64Serializer().Serialize(volumeContext)

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId: fmt.Sprintf("iscsi:%d", targetID),
			VolumeContext: map[string]string{
				"b64": serialized,
			},
		},
	}, nil
}

func (cs *server) deleteISCSIVolume(ctx context.Context, req *csi.DeleteVolumeRequest) error {
	targetID, err := strconv.ParseInt(strings.TrimPrefix(req.VolumeId, "iscsi:"), 10, 32)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "Failed parsing targetID: %+v", err)
	}

	// Data to be collected
	var auths []int
	var extent int
	var dataset string

	cl, err := newFreenasOapiClient(cs.freenas)
	if err != nil {
		return status.Error(codes.Unavailable, "creating FreenasOapi client failed")
	}

	// Collect Auths
	var targetbody []byte
	if targetbody, err = handleNasResponse(cl.GetIscsiTargetIdId(ctx, []interface{}{targetID}, &FreenasOapi.GetIscsiTargetIdIdParams{})); err != nil {
		return err
	}
	var targetresult struct {
		Groups []struct {
			Auth *int64 `json:"auth"`
		} `json:"groups"`
	}
	if err = json.Unmarshal(targetbody, &targetresult); err != nil {
		return status.Errorf(codes.Unavailable, "Error parsing result from NAS: %+v", err)
	}

	for _, group := range targetresult.Groups {
		var authbody []byte
		if authbody, err = handleNasResponse(cl.GetIscsiAuth(ctx, &FreenasOapi.GetIscsiAuthParams{}, func(ctx context.Context, req *http.Request) error {
			q := req.URL.Query()
			q.Add("tag", strconv.FormatInt(*group.Auth, 10))
			req.URL.RawQuery = q.Encode()

			return nil
		})); err != nil {
			return err
		}

		var authresult []struct {
			ID  *int   `json:"id"`
			Tag *int64 `json:"tag"`
		}
		if err = json.Unmarshal(authbody, &authresult); err != nil {
			return status.Errorf(codes.Unavailable, "Error parsing result from NAS: %+v", err)
		}

		for _, auth := range authresult {
			// extra validation in case of server-side filtering does not work
			if auth.Tag != nil && *auth.Tag == *group.Auth {
				auths = append(auths, *auth.ID)
			}
		}
	}

	// Collect extent
	if extent, err = cs.iscsiGetExtentForTarget(ctx, cl, targetID); err != nil {
		return err
	}

	// Collect dataset
	if dataset, err = cs.iscsiGetDatasetForExtent(ctx, cl, extent); err != nil {
		return err
	}

	//
	// Delete resources
	//

	// Delete target
	if _, err = handleNasResponse(cl.DeleteIscsiTargetIdId(ctx, int(targetID), false)); err != nil {
		return err
	}

	// Delete auths
	for _, auth := range auths {
		if _, err = handleNasResponse(cl.DeleteIscsiAuthIdId(ctx, auth)); err != nil {
			return err
		}
	}

	// Delete extents
	if _, err = handleNasResponse(cl.DeleteIscsiExtentIdId(ctx, extent, FreenasOapi.DeleteIscsiExtentIdIdJSONRequestBody{})); err != nil {
		return err
	}

	return cs.removeDataset(ctx, cl, dataset)
}

func (cs *server) expandISCSIVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	targetID, err := strconv.ParseInt(strings.TrimPrefix(req.VolumeId, "iscsi:"), 10, 32)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Failed parsing targetID: %+v", err)
	}

	volsize := int(req.CapacityRange.GetLimitBytes())
	if volsize == 0 {
		volsize = int(req.CapacityRange.GetRequiredBytes())
	}

	cl, err := newFreenasOapiClient(cs.freenas)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "creating FreenasOapi client failed")
	}

	// Collect extent
	var extent int
	if extent, err = cs.iscsiGetExtentForTarget(ctx, cl, targetID); err != nil {
		return nil, err
	}

	// Collect dataset
	var dataset string
	if dataset, err = cs.iscsiGetDatasetForExtent(ctx, cl, extent); err != nil {
		return nil, err
	}

	// Update volume size
	if _, err = handleNasResponse(cl.PutPoolDatasetIdId(ctx, dataset, FreenasOapi.PutPoolDatasetIdIdJSONRequestBody{
		Volsize: &volsize,
	})); err != nil {
		return nil, err
	}

	return &csi.ControllerExpandVolumeResponse{CapacityBytes: int64(volsize), NodeExpansionRequired: true}, nil
}

// iscsiGetExtentForTarget looks up extentID for target through Targetextent association
func (cs *server) iscsiGetExtentForTarget(ctx context.Context, cl *FreenasOapi.Client, targetID int64) (extent int, err error) {
	var targetextentbody []byte
	if targetextentbody, err = handleNasResponse(cl.GetIscsiTargetextent(ctx, &FreenasOapi.GetIscsiTargetextentParams{}, func(ctx context.Context, req *http.Request) error {
		q := req.URL.Query()
		q.Add("target", strconv.FormatInt(targetID, 10))
		req.URL.RawQuery = q.Encode()

		return nil
	})); err != nil {
		return
	}
	var result []struct {
		Extent *int   `json:"extent"`
		Target *int64 `json:"target"`
	}
	if err = json.Unmarshal(targetextentbody, &result); err != nil {
		err = status.Errorf(codes.Unavailable, "Error parsing result from NAS: %+v", err)

		return
	}
	if len(result) != 1 || result[0].Target == nil || *result[0].Target != targetID || result[0].Extent == nil {
		err = status.Errorf(codes.Unavailable, "Unexpected result from NAS for TargetExtent lookup: target=%d result=%+v", targetID, result)

		return
	}

	return *result[0].Extent, nil
}

// iscsiGetDatasetForExtent returns volume dataset for an extent. Returns error if not a volume.
func (cs *server) iscsiGetDatasetForExtent(ctx context.Context, cl *FreenasOapi.Client, extent int) (dataset string, err error) {
	var extentbody []byte
	if extentbody, err = handleNasResponse(cl.GetIscsiExtentIdId(ctx, []interface{}{extent}, &FreenasOapi.GetIscsiExtentIdIdParams{})); err != nil {
		return
	}
	var result struct {
		Type *string `json:"type"`
		Disk *string `json:"disk"`
	}
	if err = json.Unmarshal(extentbody, &result); err != nil {
		err = status.Errorf(codes.Unavailable, "Error parsing result from NAS: %+v", err)

		return
	}
	if result.Type == nil || *result.Type != "DISK" || result.Disk == nil || !strings.HasPrefix(*result.Disk, "zvol/") {
		err = status.Errorf(codes.Unavailable, "Unexpected extent received: %+v", result)

		return
	}

	return strings.TrimPrefix(*result.Disk, "zvol/"), nil
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
