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

var clusterNodes = []string{
	"http://192.168.1.15:8081",
	"http://192.168.1.19:8081",
	"http://192.168.1.22:8081",
}

var current int32

func getNextNode() string {
	next := atomic.AddInt32(&current, 1)
	return clusterNodes[(int(next)-1)%len(clusterNodes)]
}

func main() {
	// Diagnostic Endpoint
	http.HandleFunc("/gateway/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"gateway_status":   "ONLINE",
			"registered_nodes": clusterNodes,
			"failover_active":  true,
		})
	})

	// The Main Smart Router
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// 1. BULLETPROOF CORS: Accept any method and any header from the browser
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "*")
		w.Header().Set("Access-Control-Allow-Headers", "*")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		if r.URL.Path == "/gateway/status" {
			return
		}

		bodyBytes, _ := ioutil.ReadAll(r.Body)

		// 2. THE FIX: Increase timeout to 10 seconds!
		// Give the cluster enough time to save and replicate over Wi-Fi.
		client := &http.Client{Timeout: 10 * time.Second}

		// 3. THE FAILOVER LOOP
		for i := 0; i < len(clusterNodes); i++ {
			target := getNextNode()
			log.Printf("[➡️ ROUTING] Attempt %d: Sending to %s%s", i+1, target, r.URL.Path)

			req, err := http.NewRequest(r.Method, target+r.URL.Path, bytes.NewReader(bodyBytes))
			if err != nil {
				continue
			}

			// Copy headers from original client request
			for k, vv := range r.Header {
				for _, v := range vv {
					req.Header.Add(k, v)
				}
			}

			// Try to send it
			resp, err := client.Do(req)
			if err != nil {
				log.Printf("[⚠️ FAILOVER] Node %s is slow/down! Trying next...", target)
				continue
			}

			defer resp.Body.Close()

			// Safely pass back the Content-Type to the browser
			if ct := resp.Header.Get("Content-Type"); ct != "" {
				w.Header().Set("Content-Type", ct)
			}

			w.WriteHeader(resp.StatusCode)
			io.Copy(w, resp.Body)

			log.Printf("[✅ SUCCESS] Request handled by %s", target)
			return
		}

		// Total Cluster Failure
		log.Printf("[🚨 CRITICAL] All cluster nodes are unreachable!")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Total Cluster Failure: All nodes are offline or unreachable.",
		})
	})

	log.Println("🔒 DDB Smart API Gateway running securely on port 9000...")
	err := http.ListenAndServeTLS("0.0.0.0:9000", "cert.pem", "key.pem", nil)
	if err != nil {
		log.Fatalf("Gateway crashed: %v", err)
	}
}
