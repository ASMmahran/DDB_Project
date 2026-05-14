package main

import (
	"DDB2/server"
	"flag"
	"strings"
)

func main() {
	id := flag.String("id", "1", "Node ID (unique per node)")
	port := flag.String("port", "8081", "HTTP port this node listens on")
	peersFlag := flag.String("peers", "", "Comma-separated peer URLs")
	flag.Parse()

	var peers []string
	if *peersFlag != "" {
		peers = strings.Split(*peersFlag, ",")
	}

	n := server.NewNode(*id, *port, peers)
	n.Run()
}
