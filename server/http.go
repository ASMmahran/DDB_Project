package server

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
)

// startHTTP initializes the HTTP server, defines all routes, and starts listening for incoming requests.
func (n *Node) startHTTP() {
	mux := http.NewServeMux()

	// GET /node/status: Returns current state of the node (role, term, leader, peers) for debugging/monitoring.
	mux.HandleFunc("/node/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"role":      n.role.String(),
			"node_id":   n.ID,
			"leader_id": n.leaderID,
			"term":      n.currentTerm,
			"peers":     n.Peers,
		})
	})

	// Serve static UI files from the "./public" directory.
	mux.Handle("/", http.FileServer(http.Dir("./public")))

	// POST /raft/heartbeat: Endpoint for the Leader to assert its authority and prevent elections.
	mux.HandleFunc("/raft/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		json.NewDecoder(r.Body).Decode(&req)

		// Create a channel to wait for the consensus engine to process the heartbeat.
		reply := make(chan bool)
		n.heartbeatC <- heartbeatMsg{
			Term:     int(req["Term"].(float64)),
			LeaderID: req["LeaderID"].(string),
			reply:    reply,
		}
		// Wait for the consensus engine to reply and send the result back to the Leader.
		json.NewEncoder(w).Encode(map[string]bool{"ok": <-reply})
	})

	// POST /raft/vote: Endpoint for Candidates to request votes during an election.
	mux.HandleFunc("/raft/vote", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		json.NewDecoder(r.Body).Decode(&req)

		// Create a channel to wait for the node's voting decision.
		reply := make(chan bool)
		n.requestVote <- voteMsg{
			Term:        int(req["Term"].(float64)),
			CandidateID: req["CandidateID"].(string),
			reply:       reply,
		}
		// Send whether the vote was granted back to the Candidate.
		json.NewEncoder(w).Encode(map[string]bool{"granted": <-reply})
	})

	// Register all database operations to route through the centralized request handler.
	for _, path := range []string{
		"/db/create", "/db/drop",
		"/table/create", "/table/drop",
		"/query/insert", "/query/select",
		"/query/update", "/query/delete",
		"/query/raw",
	} {
		mux.HandleFunc(path, n.makeHandler())
	}

	log.Printf("[Node %s] Listening on :%s  (role=%s)", n.ID, n.Port, n.role)

	// Start the server in a blocking call. Note: log.Fatal will exit the program if it fails.
	log.Fatal(http.ListenAndServe(":"+n.Port, mux))

	// POST /special/task: Endpoint to offload heavy or language-specific tasks to external workers.
	mux.HandleFunc("/special/task", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		json.NewDecoder(r.Body).Decode(&req)

		workerType := req["type"].(string) // "python" or "node"
		payload := req["payload"]

		result, err := n.CallSpecialWorker(workerType, payload)
		if err != nil {
			w.WriteHeader(500)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(result)
	})
}

// makeHandler acts as a middleware bridging the concurrent HTTP requests to the sequential Raft event loop.
func (n *Node) makeHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 1. ADD CORS HEADERS: Tell the browser it is safe to send data here
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Internal-Replication")

		// 2. HANDLE PREFLIGHT: Browsers send an "OPTIONS" request first to check security
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		body, _ := ioutil.ReadAll(r.Body)
		// Channel to block this HTTP routine until the Raft engine finishes executing the command.
		reply := make(chan commandReply)

		// Send the request into the node's central event loop to avoid race conditions.
		n.commandC <- commandMsg{
			method:        r.Method,
			path:          r.URL.Path,
			body:          body,
			isReplication: r.Header.Get("X-Internal-Replication") == "true", // Flag to avoid infinite replication loops
			reply:         reply,
		}

		// Wait for the execution result and write the HTTP response.
		res := <-reply
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(res.status)
		w.Write(res.body)
	}
}
