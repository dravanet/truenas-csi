# github.com/dravanet/truenas-csi

CSI driver utilizing FreeNAS/TrueNAS for volume management.

Still in development.

## Usage

TBD

## Operation

During volume create, according to capability requests, the driver makes a decision to create an nfs or iscsi share.

If nfs was chosen, a dataset is created under `nfs.*.rootdataset`. If iscsi was chosen, a dataset is created under `iscsi.*.rootdataset`. In all cases, the full dataset will be returned as a VolumeID.

The Name in the request field will be stored, in `nfsshare.comment` or in `iscsiextent.comment`.

## NAS configuration selection

On CreateVolume request, parameters may specify which TrueNAS to use, and also select its sub-configuration (nfs or iscsi). Any of these parameters may be skipped, then `default` entries are looked up.

Parameter name | Effect
---------------|--------
truenas-csi.dravanet.net/nas | NAS Selection
truenas-csi.dravanet.net/config | Sub-configuration selection

Check [sample configuration](truenas-csi-controller.yml.sample).