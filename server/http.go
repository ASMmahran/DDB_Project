package server

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
)

func (n *Node) startHTTP() {
	mux := http.NewServeMux()

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

	mux.Handle("/", http.FileServer(http.Dir("./public")))

	mux.HandleFunc("/raft/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		json.NewDecoder(r.Body).Decode(&req)

		reply := make(chan bool)
		n.heartbeatC <- heartbeatMsg{
			Term:     int(req["Term"].(float64)),
			LeaderID: req["LeaderID"].(string),
			reply:    reply,
		}
		json.NewEncoder(w).Encode(map[string]bool{"ok": <-reply})
	})

	mux.HandleFunc("/raft/vote", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		json.NewDecoder(r.Body).Decode(&req)

		reply := make(chan bool)
		n.requestVote <- voteMsg{
			Term:        int(req["Term"].(float64)),
			CandidateID: req["CandidateID"].(string),
			reply:       reply,
		}
		json.NewEncoder(w).Encode(map[string]bool{"granted": <-reply})
	})

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
	log.Fatal(http.ListenAndServe(":"+n.Port, mux))

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
		reply := make(chan commandReply)

		n.commandC <- commandMsg{
			method:        r.Method,
			path:          r.URL.Path,
			body:          body,
			isReplication: r.Header.Get("X-Internal-Replication") == "true",
			reply:         reply,
		}

		res := <-reply
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(res.status)
		w.Write(res.body)
	}
}
