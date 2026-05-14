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

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	return [...]string{"Slave", "Candidate", "Master"}[r]
}

type Node struct {
	ID    string
	Port  string
	Peers []string

	role        Role
	currentTerm int
	votedFor    string
	leaderID    string

	engine *storage.Engine

	heartbeatC  chan heartbeatMsg
	requestVote chan voteMsg
	commandC    chan commandMsg

	mu sync.RWMutex
}

type heartbeatMsg struct {
	Term     int
	LeaderID string
	reply    chan bool
}

type voteMsg struct {
	Term        int
	CandidateID string
	reply       chan bool
}

type commandMsg struct {
	method        string
	path          string
	body          []byte
	isReplication bool
	reply         chan commandReply
}

type commandReply struct {
	status int
	body   []byte
}

func NewNode(id, port string, peers []string) *Node {
	return &Node{
		ID:          id,
		Port:        port,
		Peers:       peers,
		role:        Follower,
		engine:      storage.NewEngine(id),
		heartbeatC:  make(chan heartbeatMsg),
		requestVote: make(chan voteMsg),
		commandC:    make(chan commandMsg),
	}
}

func (n *Node) Run() {
	go n.loop()
	n.startHTTP()
}

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

func randomTimeout() time.Duration {
	return time.Duration(1500+rand.Intn(1500)) * time.Millisecond
}

func (n *Node) runFollower() {
	log.Printf("[Node %s] Follower  (term %d, leader=%s)", n.ID, n.currentTerm, n.leaderID)
	timeout := time.After(randomTimeout())

	for n.role == Follower {
		select {
		case hb := <-n.heartbeatC:
			if hb.Term >= n.currentTerm {
				n.currentTerm = hb.Term
				n.leaderID = hb.LeaderID
				timeout = time.After(randomTimeout())
				hb.reply <- true
			} else {
				hb.reply <- false
			}
		case v := <-n.requestVote:
			if v.Term > n.currentTerm && n.votedFor == "" {
				n.currentTerm = v.Term
				n.votedFor = v.CandidateID
				timeout = time.After(randomTimeout())
				v.reply <- true
			} else {
				v.reply <- false
			}
		case cmd := <-n.commandC:
			n.handleAsFollower(cmd)
		case <-timeout:
			log.Printf("[Node %s] Heartbeat timeout → starting election", n.ID)
			n.role = Candidate
		}
	}
}

func (n *Node) handleAsFollower(cmd commandMsg) {
	switch {
	case cmd.isReplication:
		n.execute(cmd)
	case cmd.path == "/query/select":
		n.execute(cmd)
	case cmd.path == "/query/raw":
		n.execute(cmd)
	case cmd.path == "/db/create":
		n.execute(cmd)
	case cmd.path == "/table/create":
		n.execute(cmd)
	case cmd.path == "/db/drop":
		cmd.reply <- commandReply{403, jsonErr("Only the Master node can drop databases")}
	case n.leaderID == "":
		cmd.reply <- commandReply{503, jsonErr("No master elected yet — please retry")}
	default:
		n.forwardToLeader(cmd)
	}
}

func (n *Node) runCandidate() {
	n.currentTerm++
	n.votedFor = n.ID
	log.Printf("[Node %s] Candidate (term %d) — requesting votes", n.ID, n.currentTerm)

	var mu sync.Mutex
	votes := 1

	for _, peer := range n.Peers {
		go func(p string) {
			body, _ := json.Marshal(map[string]interface{}{
				"Term":        n.currentTerm,
				"CandidateID": n.ID,
			})
			resp, err := http.Post(p+"/raft/vote", "application/json", bytes.NewBuffer(body))
			if err != nil {
				return
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

	timeout := time.After(randomTimeout())
	majority := (len(n.Peers)+1)/2 + 1

	for n.role == Candidate {
		mu.Lock()
		v := votes
		mu.Unlock()

		if v >= majority {
			log.Printf("[Node %s] Won election — becoming Master (term %d)", n.ID, n.currentTerm)
			n.role = Leader
			n.leaderID = n.ID
			n.votedFor = ""
			return
		}

		select {
		case hb := <-n.heartbeatC:
			if hb.Term >= n.currentTerm {
				n.currentTerm = hb.Term
			}
			n.role = Follower
			n.leaderID = hb.LeaderID
			n.votedFor = ""
			hb.reply <- true
			return
		case v := <-n.requestVote:
			if v.Term > n.currentTerm {
				n.currentTerm = v.Term
				n.votedFor = v.CandidateID
				n.role = Follower
				v.reply <- true
				return
			}
			v.reply <- false
		case cmd := <-n.commandC:
			cmd.reply <- commandReply{503, jsonErr("Election in progress — please retry")}
		case <-timeout:
			log.Printf("[Node %s] Election timed out, retrying", n.ID)
			n.votedFor = ""
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (n *Node) runLeader() {
	log.Printf("[Node %s] Master    (term %d)", n.ID, n.currentTerm)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for n.role == Leader {
		select {
		case <-ticker.C:
			n.sendHeartbeats()
		case hb := <-n.heartbeatC:
			if hb.Term > n.currentTerm {
				n.currentTerm = hb.Term
				n.leaderID = hb.LeaderID
				n.role = Follower
				hb.reply <- true
				return
			}
			hb.reply <- false
		case v := <-n.requestVote:
			v.reply <- false
		case cmd := <-n.commandC:
			n.execute(cmd)
		}
	}
}

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

func (n *Node) execute(cmd commandMsg) {
	status, body := n.dispatch(cmd)
	if status == 200 && n.role == Leader && isWrite(cmd) {
		n.replicate(cmd)
	}
	cmd.reply <- commandReply{status, body}
}

func (n *Node) dispatch(cmd commandMsg) (int, []byte) {
	var req map[string]interface{}
	if len(cmd.body) > 0 {
		json.Unmarshal(cmd.body, &req)
	}

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

	if err != nil {
		return 400, jsonErr(err.Error())
	}
	resp := map[string]interface{}{"success": true}
	if result != nil {
		resp["data"] = result
	}
	b, _ := json.Marshal(resp)
	return 200, b
}

func (n *Node) replicate(cmd commandMsg) {
	for _, peer := range n.Peers {
		go func(p string) {
			req, _ := http.NewRequest(cmd.method, p+cmd.path, bytes.NewBuffer(cmd.body))
			req.Header.Set("X-Internal-Replication", "true")
			req.Header.Set("Content-Type", "application/json")
			new(http.Client).Do(req)
		}(peer)
	}
}

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

	req, _ := http.NewRequest(cmd.method, leaderURL+cmd.path, bytes.NewBuffer(cmd.body))
	req.Header.Set("Content-Type", "application/json")

	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		cmd.reply <- commandReply{502, jsonErr("Failed to forward to master: " + err.Error())}
		return
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	buf.ReadFrom(resp.Body)
	cmd.reply <- commandReply{resp.StatusCode, buf.Bytes()}
}

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

func isWrite(cmd commandMsg) bool {
	return cmd.path != "/query/select"
}

func jsonErr(msg string) []byte {
	b, _ := json.Marshal(map[string]string{"error": msg})
	return b
}

func containsID(url, id string) bool {
	return len(id) > 0 && (bytes.Contains([]byte(url), []byte(id)) ||
		bytes.Contains([]byte(url), []byte(":"+id)))
}

func (n *Node) CallSpecialWorker(workerType string, data interface{}) (interface{}, error) {
	// Allows you to define the Python/Node worker IP dynamically.
	// If you run the worker on another PC, set WORKER_HOST=192.168.x.x before running the node.
	host := os.Getenv("WORKER_HOST")
	if host == "" {
		host = "localhost" // Defaults to local machine
	}

	url := fmt.Sprintf("http://%s:5001/process", host) // Python
	if workerType == "node" {
		url = fmt.Sprintf("http://%s:5002/process", host) // Node
	}

	body, _ := json.Marshal(map[string]interface{}{"data": data})

	client := http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("worker connection failed: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to parse worker response: %v", err)
	}
	return result, nil
}
