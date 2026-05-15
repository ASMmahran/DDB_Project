```markdown
# DDB — Highly Available Distributed Database in Go

A robust, fault-tolerant distributed database system built in Go. It features **Master/Slave replication**, **Raft-inspired automatic leader election**, a **Smart API Gateway** with auto-failover, and a fully decoupled **Remote Web Client**.

```text
       [ Remote Web Client (client.html) ]
                      |
                      v
      +-------------------------------+
      |   Smart API Gateway (Go)      |
      |   - Round-Robin Routing       |
      |   - Auto-Failover Loop        |
      |   - CORS Management           |
      +-------------------------------+
                      |
        +-------------+-------------+
        ▼             ▼             ▼
  +----------+   +----------+   +----------+
  | Node 1   |   | Node 2   |   | Node 3   |
  | (Master) |   | (Slave)  |   | (Slave)  |
  |----------|   |----------|   |----------|
  | Writes   |   | Proxy    |   | Proxy    |
  | Broadcast|   | Forward  |   | Forward  |
  +----------+   +----------+   +----------+
        |             |             |
     [MySQL]       [MySQL]       [MySQL]

```

---

## 🌟 System Architecture & Features

### 1. Smart API Gateway (`gateway.go`)

Instead of connecting directly to the database nodes, clients send requests to the Gateway.

* **Load Balancing:** Uses a Round-Robin algorithm to distribute network traffic evenly across all available nodes.
* **Auto-Failover (High Availability):** If the Gateway routes a request to a dead/offline node, it instantly catches the connection error and reroutes the payload to the next available node without dropping the client's request.
* **CORS Handling:** Automatically injects security headers to allow remote web browsers to safely transmit data across the network.

### 2. Node Roles & Consensus

* **Master:** Handles all write operations, dynamically creates sanitized databases locally, and broadcasts replication payloads to all slaves. Sends heartbeats every 500ms.
* **Slave:** Serves read queries locally. If a Slave receives a Write/Insert command, it acts as a proxy, automatically forwarding the request to the Master and returning the result to the Gateway.
* **Leader Election (Raft):** If the Master goes offline, Slaves realize the heartbeats have stopped, transition to Candidates, and hold a majority election to promote a new Master.

### 3. Decoupled Remote Client (`client.html`)

A standalone HTML/JS client that can run on any device (tablet, phone, or a separate laptop). It connects exclusively to the API Gateway and features specialized dashboards for Schema Architecture and Data Entry.

---

## 🚀 Deployment Guide (Multi-Device Cluster)

### Prerequisites

* Go 1.21+ installed.
* MySQL 8.0+ installed and running on **each** device.
* MySQL configured on `localhost:3306` with user `root` / password `root` (Or configured to point to a central DB in `engine.go`).
* Windows Firewall configured to allow inbound TCP traffic on ports `8081` (Nodes) and `9000` (Gateway).

### 1. Build the Executable

On your main development machine, compile the Go code:

```bash
go build -o DDB2.exe

```

*Distribute this `DDB2.exe` file to all three physical devices.*

### 2. Boot the Cluster (Example IPs)

Start the nodes in sequence. **Start Node 1 first and wait 5 seconds** so it wins the election and becomes the Master, then start the others. Pass the full URL as the `-id` to ensure correct network routing.

**Device 1 (192.168.1.15):**

```powershell
.\DDB2.exe -id="[http://192.168.1.15:8081](http://192.168.1.15:8081)" -port="8081" -peers="[http://192.168.1.19:8081](http://192.168.1.19:8081),[http://192.168.1.22:8081](http://192.168.1.22:8081)"

```

**Device 2 (192.168.1.19):**

```powershell
.\DDB2.exe -id="[http://192.168.1.19:8081](http://192.168.1.19:8081)" -port="8081" -peers="[http://192.168.1.15:8081](http://192.168.1.15:8081),[http://192.168.1.22:8081](http://192.168.1.22:8081)"

```

**Device 3 (192.168.1.22):**

```powershell
.\DDB2.exe -id="[http://192.168.1.22:8081](http://192.168.1.22:8081)" -port="8081" -peers="[http://192.168.1.15:8081](http://192.168.1.15:8081),[http://192.168.1.19:8081](http://192.168.1.19:8081)"

```

### 3. Start the API Gateway

On any device (or a 4th dedicated device), run the gateway to listen on port 9000:

```bash
go run gateway.go

```

*You can verify the gateway is alive by visiting `http://<GATEWAY_IP>:9000/gateway/status`.*

### 4. Connect the Remote Client

Open `client.html` on any device connected to the network. Point the "Master Node URL" to your API Gateway (e.g., `http://192.168.1.15:9000`) and begin transmitting data!

---

## 📁 Project Structure

```text
ddb_project/
├── .gitignore           Secures certificates and executables
├── DDB2.exe             Compiled Node binary
├── gateway.go           Smart API Gateway & Load Balancer
├── client.html          Standalone remote UI client
├── server/
│   ├── node.go          Node struct, Raft state machine, election logic
│   └── http.go          HTTP route registration and network forwarding
├── storage/
│   └── engine.go        MySQL storage layer & database sanitization
├── public/
│   └── index.html       Legacy onboard Web GUI (Optional)
├── workers/             
│   └── analytics.py     Bonus Python specialized task worker
└── main.go              Entry point — parses flags, starts node

```

---

## 🔌 API Reference

All endpoints accept and return JSON. The API Gateway routes these dynamically to the correct node.

### System Endpoints

* `GET /node/status` : Returns node identity, role, and current leader.
* `POST /special/task` : Routes specialized workloads to multi-stack Python/Node workers.

### Database Architecture

```json
POST /db/create    { "db": "school" }
POST /db/drop      { "db": "school" }
POST /table/create { "db": "school", "table": "students", "attributes": ["name","age"] }

```

### Data Queries

```json
POST /query/insert { "db": "school", "table": "students", "record": {"name":"Alice","age":"20"} }
POST /query/select { "db": "school", "table": "students", "query": {"age":"20"} }
POST /query/update { "db": "school", "table": "students", "query": {"name":"Alice"}, "update": {"age":"21"} }
POST /query/raw    { "db": "school", "sql": "SELECT * FROM students" }

```

```

```