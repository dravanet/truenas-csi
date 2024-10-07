package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"github.com/dravanet/truenas-csi/pkg/config"
	"github.com/dravanet/truenas-csi/pkg/controller"
	"github.com/dravanet/truenas-csi/pkg/csi"
	"github.com/dravanet/truenas-csi/pkg/identity"
	"github.com/dravanet/truenas-csi/pkg/node"

	"github.com/namsral/flag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"gopkg.in/yaml.v2"
)

const (
	unixProto = "unix://"
	tcpProto  = "tcp://"
)

func main() {
	hostname, _ := os.Hostname()

	csiEndpoint := flag.String("csi-endpoint", "unix:///csi/csi.sock", "CSI Endpoint address")
	csiNodeId := flag.String("csi-node-id", hostname, "CSI Node ID reported in NodeInfo")
	controllerConfig := flag.String("controller-config", "", "Configuration for CSI, enables Controller services")
	tlsCert := flag.String("tls-cert", "", "TLS Certificate")
	tlsKey := flag.String("tls-key", "", "TLS Private key")
	tlsCA := flag.String("tls-ca", "", "TLS Certificate Authority")

	flag.Parse()

	var controllerServer csi.ControllerServer

	if *controllerConfig != "" {
		cfgData, err := os.ReadFile(*controllerConfig)
		if err != nil {
			log.Fatal(err)
		}
		var cfg config.CSIConfiguration
		if err = yaml.Unmarshal(cfgData, &cfg); err != nil {
			log.Fatal(err)
		}

		if err = cfg.Validate(); err != nil {
			log.Fatal(err)
		}

		ser, err := yaml.Marshal(&cfg)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(ser))

		controllerServer = controller.New(cfg)
	}

	var lis net.Listener
	var err error
	var opts []grpc.ServerOption

	if *tlsCert != "" && *tlsKey != "" {
		var tlsConfig tls.Config

		cert, err := tls.LoadX509KeyPair(*tlsCert, *tlsKey)
		if err != nil {
			log.Fatal(err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}

		if *tlsCA != "" {
			roots := x509.NewCertPool()
			cacerts, err := os.ReadFile(*tlsCA)
			if err != nil {
				log.Fatal(err)
			}
			roots.AppendCertsFromPEM(cacerts)

			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
			tlsConfig.ClientCAs = roots
		}

		opts = append(opts, grpc.Creds(credentials.NewTLS(&tlsConfig)))
	}

	if strings.HasPrefix(*csiEndpoint, tcpProto) {
		address := strings.TrimPrefix(*csiEndpoint, tcpProto)

		lis, err = net.Listen("tcp", address)
	} else if strings.HasPrefix(*csiEndpoint, unixProto) {
		address := strings.TrimPrefix(*csiEndpoint, unixProto)
		if err = os.Remove(address); err != nil && !os.IsNotExist(err) {
			log.Fatalf("Failed removing existing socket: %+v", err)
		}

		lis, err = net.Listen("unix", address)
	} else {
		log.Fatalf("Only %s or %s endpoints are supported", unixProto, tcpProto)
	}

	if err != nil {
		log.Fatal(err)
	}

	server := grpc.NewServer(opts...)

	identityServer := identity.New(controllerServer != nil)
	csi.RegisterIdentityServer(server, identityServer)

	if controllerServer != nil {
		csi.RegisterControllerServer(server, controllerServer)
	}

	nodeServer := node.New(*csiNodeId)
	csi.RegisterNodeServer(server, nodeServer)

	server.Serve(lis)
}
