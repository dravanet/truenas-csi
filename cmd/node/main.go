package main

import (
	"fmt"
	"io/ioutil"
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
	"gopkg.in/yaml.v2"
)

const (
	unixProto = "unix://"
)

func main() {
	csiEndpoint := flag.String("csi-endpoint", "unix:///csi/csi.sock", "CSI Endpoint address")
	csiConfig := flag.String("csi-config", "/config/csi-config.yml", "Configuration for CSI")

	flag.Parse()

	cfgData, err := ioutil.ReadFile(*csiConfig)
	if err != nil {
		log.Fatal(err)
	}
	var cfg config.FreeNAS
	if err = yaml.Unmarshal(cfgData, &cfg); err != nil {
		log.Fatal(err)
	}

	ser, err := yaml.Marshal(&cfg)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(ser))

	if !strings.HasPrefix(*csiEndpoint, unixProto) {
		log.Fatalf("Only %s endpoints are supported", unixProto)
	}

	address := strings.TrimPrefix(*csiEndpoint, unixProto)
	if err := os.Remove(address); err != nil && !os.IsNotExist(err) {
		log.Fatalf("Failed removing existing socket: %+v", err)
	}

	lis, err := net.Listen("unix", address)
	if err != nil {
		log.Fatal(err)
	}

	server := grpc.NewServer()

	identityServer := identity.New()
	csi.RegisterIdentityServer(server, identityServer)

	nodeServer := node.New()
	csi.RegisterNodeServer(server, nodeServer)

	controllerServer := controller.New(&cfg)
	csi.RegisterControllerServer(server, controllerServer)

	server.Serve(lis)
}
