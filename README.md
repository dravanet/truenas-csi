# github.com/dravanet/truenas-csi

CSI driver utilizing FreeNAS/TrueNAS for volume management.

Still in development.

## Usage

TBD

## Operation

During volume create, according to capability requests, the driver makes a decision to use nfs or iscsi.

If nfs was chosen, a dataset is created under `nfs.*.rootdataset`. If iscsi was chosen, a dataset is created under `iscsi.*.rootdataset`. In all cases, the full dataset will be returned as a VolumeID.

The Name in the request field will be stored, in `nfsshare.comment` or in `iscsiextent.comment`.
