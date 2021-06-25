# github.com/dravanet/truenas-csi

CSI driver utilizing TrueNAS for volume management.

Warning: still in __ALPHA__ state.

## Deployment

For deployment, see [truenas-csi-chart](https://github.com/dravanet/truenas-csi-chart).

## Goals

- Use TrueNAS API only.
- Meet CSI requirements as much as can
  - idempotency during volume operations
  - implement as much capabilities

## Capabilities

The following capabilities are supported from CSI Specification 1.3.0:

Plugin:
- online volume expansion

Controller:
- create/delete volume
- expand-volume

Node:
- stage-unstage volume
- get volume stats
- expand volume

## Configuration

__TL;DR__ see [examples](examples).

The driver needs configuration to access one or more TrueNAS instances. Check [config.go](pkg/config/config.go) for full configuration structure.

The configuration has `yaml` syntax, must be passed to the application with `-controller-config` argument. This enables controller services.

The configuration file format is:
```yaml
truenas-1: <truenas-config>
backup: <truenas-config>
default: <truenas-config>
```

A `truenas-config` has the structure:
```
apiurl: <url of truenas api>
[username: <username for api access>]
[password: <password for api access>]
[apikey: <api key for api access>]
[nfs: <nfs configuration>]
[iscsi: <iscsi configuration>]
configurations:
  sub-config-1: <configuration>
  default: <configuration>
```

`apikey` is recommended over `username`+`password`.

`nfs` configuration has the strucure:
```yaml
server: <server address>
[allowedhosts: [array of allowed hosts to access share]]
[allowednetworks: [array of allowed networks to access share]]
```

`iscsi` configuration has the structure:
```yaml
portal: <portal address>
portalid: <portal id in TrueNAS>
```

Each `configuration` section has the structure:
```yaml
dataset: <root dataset>
deletePolicy: [delete|retain]
[sparse: [true|false]]
[nfs: <nfs sub-configuration>]
[iscsi: <iscsi sub-configuration>]
```

## Detailed operation

During volume create, evaluating capability requests, the driver makes a decision to create an nfs or an iscsi share.

Then a dataset is created under the selected `configuration` section. If nfs was chosen, an nfs export is created according to the selected configuration's nfs section. If iscsi was chosen, a new secret/target is created according to the selected configuration's iscsi section. Then, connection parameters are returned in the volume_context.

## NAS configuration selection

On CreateVolume request, parameters may specify which TrueNAS to use, and may select its sub-configuration. Any of these parameters may be omitted, then `default` entries are looked up.

Parameter name | Effect
---------------|--------
truenas-csi.dravanet.net/nas | NAS Selection
truenas-csi.dravanet.net/config | Sub-configuration selection

## Related projects

- [TrueNAS](https://www.truenas.com/) itself is the NAS solution
- [democratic-csi](https://github.com/democratic-csi/democratic-csi) is an existing implementation
- [ganeti-extstorage-csi](https://github.com/dravanet/ganeti-extstorage-csi) is a Ganeti external storage driver utilizing CSI
