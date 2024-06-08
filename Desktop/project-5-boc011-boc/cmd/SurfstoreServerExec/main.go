package main

import (
	"cse224/proj5/pkg/surfstore"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc"
)

// Usage String
const USAGE_STRING = "./run-server.sh -s <service_type> -p <port> -l -d (blockStoreAddr*)"

// Set of valid services
var SERVICE_TYPES = map[string]bool{"meta": true, "block": true, "both": true}

// Exit codes
const EX_USAGE int = 64

func main() {
	// Custom flag Usage message
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage of %s:\n", USAGE_STRING)
		flag.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(w, "  -%s: %v\n", f.Name, f.Usage)
		})
		fmt.Fprintf(w, "  (blockStoreAddr*): BlockStore Address (include self if service type is both)\n")
	}

	// Parse command-line argument flags
	service := flag.String("s", "", "(required) Service Type of the Server: meta, block, both")
	port := flag.Int("p", 8080, "(default = 8080) Port to accept connections")
	localOnly := flag.Bool("l", false, "Only listen on localhost")
	debug := flag.Bool("d", false, "Output log statements")
	flag.Parse()

	// Use tail arguments to hold BlockStore address
	args := flag.Args()
	var blockStoreAddrs []string

	for i := 0; i < len(args); i++ {
		blockStoreAddrs = append(blockStoreAddrs, args[i])
	}

	// Valid service type argument
	if _, ok := SERVICE_TYPES[strings.ToLower(*service)]; !ok {
		flag.Usage()
		log.Println("error for invalid service type")
		os.Exit(EX_USAGE)
	}

	// Add localhost if necessary
	addr := ""
	if *localOnly {
		addr += "localhost"
	}
	addr += ":" + strconv.Itoa(*port)

	// Disable log outputs if debug flag is missing
	if !(*debug) {
		log.SetFlags(0)
		log.SetOutput(io.Discard)
	}

	log.Fatal(startServer(addr, strings.ToLower(*service), blockStoreAddrs))
}

func startServer(hostAddr string, serviceType string, blockStoreAddrs []string) error {
	//Start server
	switch serviceType {
	case "meta":
		return startMetaServer(hostAddr, blockStoreAddrs)
	case "block":
		return startBlockServer(hostAddr, blockStoreAddrs)
	case "both":
		return startBothServers(hostAddr, blockStoreAddrs)
	}

	return nil
}

func startMetaServer(hostAddr string, blockStoreAddrs []string) error {
	server := grpc.NewServer()

	surfstore.RegisterMetaStoreServer(server, surfstore.NewMetaStore(blockStoreAddrs))

	listen, e := net.Listen("tcp", hostAddr)
	if e != nil {
		log.Println("failed to listen")
		return e
	}
	return server.Serve(listen)
}

func startBlockServer(hostAddr string, _ []string) error {
	server := grpc.NewServer()

	surfstore.RegisterBlockStoreServer(server, surfstore.NewBlockStore())

	listen, e := net.Listen("tcp", hostAddr)
	if e != nil {
		log.Println("failed to listen")
		return e
	}
	return server.Serve(listen)
}

func startBothServers(hostAddr string, blockStoreAddrs []string) error {
	server := grpc.NewServer()

	surfstore.RegisterMetaStoreServer(server, surfstore.NewMetaStore(blockStoreAddrs))
	surfstore.RegisterBlockStoreServer(server, surfstore.NewBlockStore())

	listen, e := net.Listen("tcp", hostAddr)
	if e != nil {
		log.Println("failed to listen")
		return e
	}
	return server.Serve(listen)
}
