package main

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

// clusterNodes defines the hardcoded list of backend database nodes.
// In a production environment, this might be dynamically loaded or discovered.
var clusterNodes = []string{
	"http://192.168.1.15:8081",
	"http://192.168.1.19:8081",
	"http://192.168.1.22:8081",
}

// current acts as a thread-safe counter for the Round-Robin load balancing algorithm.
var current int32

// getNextNode implements a basic Round-Robin selection strategy.
// It uses atomic operations to safely increment the counter across concurrent HTTP requests.
func getNextNode() string {
	// Atomically add 1 to current to prevent race conditions when multiple requests hit the gateway
	next := atomic.AddInt32(&current, 1)
	// Use modulo arithmetic to wrap the index back to 0 when it exceeds the length of the slice
	return clusterNodes[(int(next)-1)%len(clusterNodes)]
}

func main() {
	// Diagnostic Endpoint
	// Provides a quick way to check if the load balancer itself is alive and what nodes it knows about.
	http.HandleFunc("/gateway/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"gateway_status":   "ONLINE",
			"registered_nodes": clusterNodes,
			"failover_active":  true,
		})
	})

	// The Main Smart Router / Reverse Proxy Handler
	// This captures all traffic not matched by specific routes and forwards it to the cluster.
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// 1. BULLETPROOF CORS: Accept any method and any header from the browser
		// This ensures web-based frontends can interact with the API without cross-origin blocks.
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "*")
		w.Header().Set("Access-Control-Allow-Headers", "*")

		// Handle CORS Preflight requests. Browsers send an OPTIONS request before mutating data (POST/PUT/DELETE).
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Prevent an infinite loop if someone requests /gateway/status but it somehow falls through to here.
		if r.URL.Path == "/gateway/status" {
			return
		}

		// Read the entire body into memory so we can retry sending it if the first node fails.
		// A standard request body can only be read once, so we buffer it into bytes.
		bodyBytes, _ := ioutil.ReadAll(r.Body)

		// 2. THE FIX: Increase timeout to 10 seconds!
		// Give the cluster enough time to save and replicate over Wi-Fi.
		// This prevents the gateway from dropping the connection while the Raft consensus is still working.
		client := &http.Client{Timeout: 10 * time.Second}

		// 3. THE FAILOVER LOOP
		// Iterate up to the total number of nodes. If one fails, it immediately tries the next.
		for i := 0; i < len(clusterNodes); i++ {
			target := getNextNode() // Select the next node via Round-Robin
			log.Printf("[➡️ ROUTING] Attempt %d: Sending to %s%s", i+1, target, r.URL.Path)

			// Create a new outbound request to the target node, creating a new reader from our buffered body.
			req, err := http.NewRequest(r.Method, target+r.URL.Path, bytes.NewReader(bodyBytes))
			if err != nil {
				continue // If request creation fails, skip to the next node
			}

			// Copy all headers from the original client request into our forwarded request
			for k, vv := range r.Header {
				for _, v := range vv {
					req.Header.Add(k, v)
				}
			}

			// Try to send the request to the target node
			resp, err := client.Do(req)
			if err != nil {
				// If there's a network error or timeout, log it and let the loop try the next node
				log.Printf("[⚠️ FAILOVER] Node %s is slow/down! Trying next...", target)
				continue
			}

			// Ensure the target node's response body is closed when we are done
			defer resp.Body.Close()

			// Safely pass back the Content-Type from the backend node to the browser
			if ct := resp.Header.Get("Content-Type"); ct != "" {
				w.Header().Set("Content-Type", ct)
			}

			// Write the HTTP status code (e.g., 200 OK, 404 Not Found) returned by the backend
			w.WriteHeader(resp.StatusCode)

			// Stream the response body directly back to the original client
			io.Copy(w, resp.Body)

			log.Printf("[✅ SUCCESS] Request handled by %s", target)
			return // Success! Exit the handler so we don't try the remaining nodes.
		}

		// Total Cluster Failure
		// If the loop finishes and we haven't returned yet, it means every single node failed.
		log.Printf("[🚨 CRITICAL] All cluster nodes are unreachable!")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway) // Return HTTP 502
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Total Cluster Failure: All nodes are offline or unreachable.",
		})
	})

	// Start the HTTPS server
	log.Println("🔒 DDB Smart API Gateway running securely on port 9000...")
	// ListenAndServeTLS requires a certificate and private key to enable SSL/TLS encryption.
	err := http.ListenAndServeTLS("0.0.0.0:9000", "cert.pem", "key.pem", nil)
	if err != nil {
		log.Fatalf("Gateway crashed: %v", err) // Exit the program if the server fails to start
	}
}
