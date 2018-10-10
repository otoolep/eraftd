package main

import (
	"fmt"
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/otoolep/eraftd/http"
	"github.com/otoolep/eraftd/store"
)

// Command line defaults
const (
	DefaultHTTPAddr = ":11000"
	DefaultRaftAddr = ":12000"
)

// Command line parameters
var inmem bool
var httpAddr string
var raftAddr string
var joinAddr string
var nodeID string

func init() {
	flag.StringVar(&httpAddr, "haddr", DefaultHTTPAddr, "Set the HTTP bind address")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <raft-data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()


	if flag.NArg() == 0 {
		fmt.Fprintf(os.Stderr, "No Raft storage directory specified\n")
		os.Exit(1)
	}

	s := store.New()
	if err := s.Open(); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	h := httpd.New(httpAddr, s)
	if err := h.Start(); err != nil {
		log.Fatalf("failed to start HTTP service: %s", err.Error())
	}

	log.Println("eraftd started successfully")

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.Println("eraftd exiting")
}