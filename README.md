# DDB — Distributed Database in Go

A minimal distributed database built in Go, featuring **master/slave replication**, **automatic leader election** (Raft-inspired), and a **web-based GUI console**.

```
+---------------------------+
|       Master Node         |
|---------------------------|
| - DB Write Access         |
| - Broadcast to Slaves     |
+---------------------------+
             |
      +------+------+
      ▼             ▼
+----------+   +----------+
| Slave    |   | Slave    |
|----------|   |----------|
| Read DB  |   | Read DB  |
| Forward  |   | Forward  |
| Writes   |   | Writes   |
+----------+   +----------+
```

---

## Requirements

| Tool    | Version |
|---------|---------|
| Go      | 1.21+   |
| MySQL   | 8.0+    |

MySQL must be running on `localhost:3306` with user `root` / password `root`.

---

## Quick Start

### 1. Install dependency
```bash
go mod tidy
```

### 2. Start three nodes (in separate terminals)
```bash
# Terminal 1 — Node 1
go run . -id=1 -port=8081

# Terminal 2 — Node 2
go run . -id=2 -port=8082 -peers=

# Terminal 3 — Node 3
go run . -id=3 -port=8083 -peers=http://localhost:8081,http://localhost:8082
```

After ~2 seconds one node will win the election and print:
```
[Node X] Master (term 1)
```

### 3. Open the GUI
Navigate to **http://localhost:8081** (or whichever port you prefer).

---

## API Reference

All endpoints accept and return JSON.

### Status
```
GET  /node/status
```
Returns the node's current role, term, and leader ID.

### Databases *(Master only)*
```
POST /db/create    { "db": "school" }
POST /db/drop      { "db": "school" }
```

### Tables *(Master only for create)*
```
POST /table/create  { "db":"school", "table":"students", "attributes":["name","age"] }
POST /table/drop    { "db":"school", "table":"students" }
```

### Queries *(all nodes)*
```
POST /query/insert  { "db":"school", "table":"students", "record":{"name":"Alice","age":"20"} }
POST /query/select  { "db":"school", "table":"students", "query":{"age":"20"} }
POST /query/update  { "db":"school", "table":"students", "query":{"name":"Alice"}, "update":{"age":"21"} }
POST /query/delete  { "db":"school", "table":"students", "query":{"name":"Alice"} }
POST /query/raw     { "db":"school", "sql":"SELECT * FROM students" }
```

---

## Architecture

### Node Roles
| Role      | Behaviour                                              |
|-----------|--------------------------------------------------------|
| Master    | Handles all writes, replicates to slaves, sends heartbeats every 500 ms |
| Slave     | Serves reads locally, forwards writes to master         |
| Candidate | Temporary — requests votes, becomes master if it wins majority |

### Leader Election
1. Every slave has a random timeout (1.5 – 3 s).  
2. If no heartbeat arrives before the timeout, the slave becomes a **Candidate**, increments its term, and requests votes from peers.  
3. First candidate to collect a **majority** of votes becomes the new **Master**.  
4. The new master immediately starts sending heartbeats, stopping other elections.

### Replication
- The master executes the write locally first.  
- On success it fans out the same request to every peer with the `X-Internal-Replication: true` header.  
- Replication is **asynchronous** — the client gets a reply once the master commits.

### Write Forwarding
A slave that receives a write request automatically forwards it to the master, then relays the response to the client transparently.

### Storage
Each node stores its data in isolated MySQL databases prefixed with the node ID:
```
node1_school   (Node 1's copy of "school")
node2_school   (Node 2's copy of "school")
node3_school   (Node 3's copy of "school")
```

---

## Automated Tests

```powershell
# Start all three nodes first, then run:
.\test_cluster.ps1
```

The script tests: create DB/table, insert via master and slave, select from slaves (replication check), update, delete, access control, raw SQL, and cleanup.

---

## File Structure

```
ddb/
├── main.go              Entry point — parses flags, starts node
├── go.mod
├── server/
│   ├── node.go          Node struct, Raft state machine, election logic
│   └── http.go          HTTP route registration and handlers
├── storage/
│   └── engine.go        MySQL storage layer
├── public/
│   └── index.html       Web GUI console
└── test_cluster.ps1     PowerShell integration test
```
