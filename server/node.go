package server

import (
	"DDB2/storage"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// Role represents the current Raft consensus state of the node.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

// String representation of Roles for logging and API outputs.
func (r Role) String() string {
	return [...]string{"Slave", "Candidate", "Master"}[r]
}

// Node represents a single server in the distributed database cluster.
type Node struct {
	ID    string
	Port  string
	Peers []string // List of URLs/IPs of other nodes in the cluster

	// Raft state variables
	role        Role
	currentTerm int    // Current election term number
	votedFor    string // ID of the candidate this node voted for in the current term
	leaderID    string // ID of the currently recognized leader

	engine *storage.Engine // The underlying local MySQL database engine

	// Communication channels for the main event loop
	heartbeatC  chan heartbeatMsg
	requestVote chan voteMsg
	commandC    chan commandMsg

	mu sync.RWMutex
}

// heartbeatMsg represents an incoming append-entries/heartbeat from the Leader.
type heartbeatMsg struct {
	Term     int
	LeaderID string
	reply    chan bool // Channel to send success/failure back to the HTTP handler
}

// voteMsg represents a request from a Candidate to vote for them.
type voteMsg struct {
	Term        int
	CandidateID string
	reply       chan bool // Channel to send granted/denied back to the HTTP handler
}

// commandMsg encapsulates an HTTP request to be processed synchronously by the node.
type commandMsg struct {
	method        string
	path          string
	body          []byte
	isReplication bool              // Indicates if this command came from the Leader replicating data
	reply         chan commandReply // Channel to send the final HTTP response back
}

// commandReply encapsulates the HTTP response to be sent back to the client/peer.
type commandReply struct {
	status int
	body   []byte
}

// NewNode initializes a new Node struct with necessary channels and the storage engine.
func NewNode(id, port string, peers []string) *Node {
	return &Node{
		ID:          id,
		Port:        port,
		Peers:       peers,
		role:        Follower, // All nodes start as Followers
		engine:      storage.NewEngine(id),
		heartbeatC:  make(chan heartbeatMsg),
		requestVote: make(chan voteMsg),
		commandC:    make(chan commandMsg),
	}
}

// Run starts the node's background event loop and its HTTP server.
func (n *Node) Run() {
	go n.loop()
	n.startHTTP()
}

// loop is the core state machine of the node. It switches behavior based on its Role.
func (n *Node) loop() {
	for {
		switch n.role {
		case Follower:
			n.runFollower()
		case Candidate:
			n.runCandidate()
		case Leader:
			n.runLeader()
		}
	}
}

// randomTimeout generates a randomized election timeout between 1.5s and 3.0s.
// Randomization prevents split votes where multiple followers become candidates simultaneously.
func randomTimeout() time.Duration {
	return time.Duration(1500+rand.Intn(1500)) * time.Millisecond
}

// runFollower contains the logic for a node operating in the Follower state.
func (n *Node) runFollower() {
	log.Printf("[Node %s] Follower  (term %d, leader=%s)", n.ID, n.currentTerm, n.leaderID)
	timeout := time.After(randomTimeout()) // Start the election countdown timer

	for n.role == Follower {
		select {
		case hb := <-n.heartbeatC:
			// If heartbeat is from a valid or newer term leader, acknowledge it.
			if hb.Term >= n.currentTerm {
				n.currentTerm = hb.Term
				n.leaderID = hb.LeaderID
				timeout = time.After(randomTimeout()) // Reset election timer
				hb.reply <- true
			} else {
				hb.reply <- false // Reject outdated heartbeats
			}
		case v := <-n.requestVote:
			// Vote for a candidate if their term is newer and we haven't voted yet.
			if v.Term > n.currentTerm && n.votedFor == "" {
				n.currentTerm = v.Term
				n.votedFor = v.CandidateID
				timeout = time.After(randomTimeout()) // Reset election timer
				v.reply <- true
			} else {
				v.reply <- false
			}
		case cmd := <-n.commandC:
			// Process incoming user requests or replication requests from the Leader.
			n.handleAsFollower(cmd)
		case <-timeout:
			// If timer runs out without receiving a heartbeat, start an election.
			log.Printf("[Node %s] Heartbeat timeout → starting election", n.ID)
			n.role = Candidate
		}
	}
}

// handleAsFollower routes incoming commands appropriately while the node is a Follower.
func (n *Node) handleAsFollower(cmd commandMsg) {
	switch {
	case cmd.isReplication:
		// Accept writes that are replicated from the Leader.
		n.execute(cmd)
	case cmd.path == "/query/select":
		// Allow reading directly from the local follower (eventual consistency).
		n.execute(cmd)
	case cmd.path == "/query/raw":
		// Allow raw queries directly (assumes they are non-mutating or safe).
		n.execute(cmd)
	case cmd.path == "/db/create":
		// Database creation is permitted on followers directly.
		n.execute(cmd)
	case cmd.path == "/table/create":
		// Table creation is permitted on followers directly.
		n.execute(cmd)
	case cmd.path == "/db/drop":
		// Restrict dropping databases to the Master only.
		cmd.reply <- commandReply{403, jsonErr("Only the Master node can drop databases")}
	case n.leaderID == "":
		// If a client sends a write request during an election, reject it.
		cmd.reply <- commandReply{503, jsonErr("No master elected yet — please retry")}
	default:
		// Forward any mutating requests (INSERT, UPDATE, DELETE) to the Leader.
		n.forwardToLeader(cmd)
	}
}

// runCandidate contains the logic for a node actively trying to win an election.
func (n *Node) runCandidate() {
	n.currentTerm++   // Start a new election term
	n.votedFor = n.ID // Vote for self
	log.Printf("[Node %s] Candidate (term %d) — requesting votes", n.ID, n.currentTerm)

	var mu sync.Mutex
	votes := 1 // Start with 1 vote (self)

	// Broadcast vote requests to all peers asynchronously.
	for _, peer := range n.Peers {
		go func(p string) {
			body, _ := json.Marshal(map[string]interface{}{
				"Term":        n.currentTerm,
				"CandidateID": n.ID,
			})
			resp, err := http.Post(p+"/raft/vote", "application/json", bytes.NewBuffer(body))
			if err != nil {
				return // Peer might be down
			}
			defer resp.Body.Close()
			var result map[string]bool
			json.NewDecoder(resp.Body).Decode(&result)
			if result["granted"] {
				mu.Lock()
				votes++
				mu.Unlock()
			}
		}(peer)
	}

	timeout := time.After(randomTimeout()) // Start candidate timeout
	majority := (len(n.Peers)+1)/2 + 1     // Calculate minimum votes required to win

	for n.role == Candidate {
		mu.Lock()
		v := votes
		mu.Unlock()

		// If we secured the majority of votes, become the Leader.
		if v >= majority {
			log.Printf("[Node %s] Won election — becoming Master (term %d)", n.ID, n.currentTerm)
			n.role = Leader
			n.leaderID = n.ID
			n.votedFor = ""
			return
		}

		select {
		case hb := <-n.heartbeatC:
			// If we receive a heartbeat from a valid Leader while campaigning, concede defeat.
			if hb.Term >= n.currentTerm {
				n.currentTerm = hb.Term
			}
			n.role = Follower
			n.leaderID = hb.LeaderID
			n.votedFor = ""
			hb.reply <- true
			return
		case v := <-n.requestVote:
			// If another candidate with a newer term asks for a vote, step down and vote for them.
			if v.Term > n.currentTerm {
				n.currentTerm = v.Term
				n.votedFor = v.CandidateID
				n.role = Follower
				v.reply <- true
				return
			}
			v.reply <- false
		case cmd := <-n.commandC:
			// Reject client requests while the cluster is leaderless.
			cmd.reply <- commandReply{503, jsonErr("Election in progress — please retry")}
		case <-timeout:
			// If the election times out without a winner (split vote), restart the election loop.
			log.Printf("[Node %s] Election timed out, retrying", n.ID)
			n.votedFor = ""
			return
		case <-time.After(50 * time.Millisecond):
			// Brief sleep to avoid a tight busy-loop while waiting for votes
		}
	}
}

// runLeader contains the logic for the node acting as the master node.
func (n *Node) runLeader() {
	log.Printf("[Node %s] Master    (term %d)", n.ID, n.currentTerm)
	ticker := time.NewTicker(500 * time.Millisecond) // Heartbeat interval
	defer ticker.Stop()

	for n.role == Leader {
		select {
		case <-ticker.C:
			// Broadcast authority periodically to prevent follower timeouts.
			n.sendHeartbeats()
		case hb := <-n.heartbeatC:
			// If we see a heartbeat with a newer term, a new Leader exists. Step down immediately.
			if hb.Term > n.currentTerm {
				n.currentTerm = hb.Term
				n.leaderID = hb.LeaderID
				n.role = Follower
				hb.reply <- true
				return
			}
			hb.reply <- false
		case v := <-n.requestVote:
			// Reject votes for older/current terms, we are already the leader.
			v.reply <- false
		case cmd := <-n.commandC:
			// Process incoming client requests directly.
			n.execute(cmd)
		}
	}
}

// sendHeartbeats broadcasts the leader's state to all peers.
func (n *Node) sendHeartbeats() {
	body, _ := json.Marshal(map[string]interface{}{
		"Term":     n.currentTerm,
		"LeaderID": n.ID,
	})
	for _, peer := range n.Peers {
		go func(p string) {
			http.Post(p+"/raft/heartbeat", "application/json", bytes.NewBuffer(body))
		}(peer)
	}
}

// execute processes a command locally and replicates it if necessary.
func (n *Node) execute(cmd commandMsg) {
	status, body := n.dispatch(cmd) // Execute on the local storage engine

	// If execution succeeded, and we are the Leader, and it's a mutating command, replicate it.
	if status == 200 && n.role == Leader && isWrite(cmd) {
		n.replicate(cmd)
	}
	cmd.reply <- commandReply{status, body}
}

// dispatch routes the parsed HTTP request to the specific storage engine function.
func (n *Node) dispatch(cmd commandMsg) (int, []byte) {
	var req map[string]interface{}
	if len(cmd.body) > 0 {
		json.Unmarshal(cmd.body, &req)
	}

	// Extract standard variables expected by most engine functions
	var db, table string
	if d, ok := req["db"].(string); ok {
		db = d
	}
	if t, ok := req["table"].(string); ok {
		table = t
	}

	var (
		result interface{}
		err    error
	)

	// Call the underlying MySQL wrapper based on the requested endpoint path.
	switch cmd.path {
	case "/db/create":
		err = n.engine.CreateDB(db)
	case "/db/drop":
		err = n.engine.DropDB(db)
	case "/table/create":
		var attrs []string
		if a, ok := req["attributes"].([]interface{}); ok {
			for _, v := range a {
				attrs = append(attrs, fmt.Sprintf("%v", v))
			}
		}
		err = n.engine.CreateTable(db, table, attrs)
	case "/table/drop":
		err = n.engine.DropTable(db, table)
	case "/query/insert":
		record, _ := req["record"].(map[string]interface{})
		err = n.engine.Insert(db, table, record)
	case "/query/select":
		query, _ := req["query"].(map[string]interface{})
		result, err = n.engine.Select(db, table, query)
	case "/query/update":
		where, _ := req["query"].(map[string]interface{})
		set, _ := req["update"].(map[string]interface{})
		result, err = n.engine.Update(db, table, where, set)
	case "/query/delete":
		query, _ := req["query"].(map[string]interface{})
		result, err = n.engine.Delete(db, table, query)
	case "/query/raw":
		rawSQL, _ := req["sql"].(string)
		result, err = n.engine.ExecRaw(db, rawSQL)
	default:
		return 404, jsonErr("Unknown endpoint: " + cmd.path)
	}

	// Format errors into JSON responses
	if err != nil {
		return 400, jsonErr(err.Error())
	}

	// Format successful responses
	resp := map[string]interface{}{"success": true}
	if result != nil {
		resp["data"] = result
	}
	b, _ := json.Marshal(resp)
	return 200, b
}

// replicate broadcasts a command to all peers with a special header to avoid loops.
func (n *Node) replicate(cmd commandMsg) {
	for _, peer := range n.Peers {
		go func(p string) {
			req, _ := http.NewRequest(cmd.method, p+cmd.path, bytes.NewBuffer(cmd.body))
			req.Header.Set("X-Internal-Replication", "true") // Crucial flag for followers
			req.Header.Set("Content-Type", "application/json")
			new(http.Client).Do(req) // Fire and forget (basic implementation)
		}(peer)
	}
}

// forwardToLeader acts as a proxy, sending a Follower's received request directly to the Master.
func (n *Node) forwardToLeader(cmd commandMsg) {
	leaderURL := n.leaderURL()
	if leaderURL == "" {
		cmd.reply <- commandReply{503, jsonErr("Cannot locate master node")}
		return
	}

	// Ensure the URL is properly formatted
	if !strings.HasPrefix(leaderURL, "http") {
		leaderURL = "http://" + leaderURL
	}

	// Reconstruct the client's request aimed at the Master
	req, _ := http.NewRequest(cmd.method, leaderURL+cmd.path, bytes.NewBuffer(cmd.body))
	req.Header.Set("Content-Type", "application/json")

	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		cmd.reply <- commandReply{502, jsonErr("Failed to forward to master: " + err.Error())}
		return
	}
	defer resp.Body.Close()

	// Read the Master's response and send it back to our local HTTP handler to return to the client.
	var buf bytes.Buffer
	buf.ReadFrom(resp.Body)
	cmd.reply <- commandReply{resp.StatusCode, buf.Bytes()}
}

// leaderURL attempts to format the recognized LeaderID into a usable HTTP URL.
func (n *Node) leaderURL() string {
	// If the leader ID is already a full network address (e.g., 192.168.1.15:8080)
	if strings.Contains(n.leaderID, ":") || strings.HasPrefix(n.leaderID, "http") {
		return n.leaderID
	}

	// Fallback to searching the peers list (useful for localhost testing)
	for _, p := range n.Peers {
		if containsID(p, n.leaderID) {
			return p
		}
	}

	// If we still can't find it, return the raw leader ID as a last resort
	return n.leaderID
}

// isWrite determines if an API path mutates the state of the database.
func isWrite(cmd commandMsg) bool {
	return cmd.path != "/query/select" // Simplistic check: everything but select is a write
}

// jsonErr is a helper function to quickly format error messages into standard JSON bytes.
func jsonErr(msg string) []byte {
	b, _ := json.Marshal(map[string]string{"error": msg})
	return b
}

// containsID is a helper string matcher used to map node IDs to Peer URLs.
func containsID(url, id string) bool {
	return len(id) > 0 && (bytes.Contains([]byte(url), []byte(id)) ||
		bytes.Contains([]byte(url), []byte(":"+id)))
}

// CallSpecialWorker proxies requests to an external processing service (like a Python data science or Node.js backend).
func (n *Node) CallSpecialWorker(workerType string, data interface{}) (interface{}, error) {
	// Allows you to define the Python/Node worker IP dynamically.
	// If you run the worker on another PC, set WORKER_HOST=192.168.x.x before running the node.
	host := os.Getenv("WORKER_HOST")
	if host == "" {
		host = "localhost" // Defaults to local machine
	}

	// Map requested worker types to specific external ports
	url := fmt.Sprintf("http://%s:5001/process", host) // Python
	if workerType == "node" {
		url = fmt.Sprintf("http://%s:5002/process", host) // Node
	}

	body, _ := json.Marshal(map[string]interface{}{"data": data})

	// Make the remote HTTP call
	client := http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("worker connection failed: %v", err)
	}
	defer resp.Body.Close()

	// Parse and return the external worker's result
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to parse worker response: %v", err)
	}
	return result, nil
}
