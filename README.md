# Distributed Banking System with Raft Consensus

A distributed banking platform in Go implementing a modified Raft consensus protocol
to maintain strongly consistent account balances across a Redis cluster.

## Architecture

```
Client → HTTP API → Raft Node → Redis (per node)
                        ↕
               Raft RPC (vote/append)
                        ↕
              Other Nodes (2 followers)
```

## Run the Cluster

```bash
# Install dependencies
go mod tidy

# Start the 3-node cluster (requires Docker)
docker-compose up --build

# Or run a single node locally (for dev)
NODE_ID=node1 PORT=8080 REDIS_ADDR=localhost:6379 PEERS="" go run ./cmd/node
```

## API Usage

### 1. Get a token (login)

```bash
# Admin token
curl -X POST http://localhost:8080/login \
  -H "Content-Type: application/json" \
  -d '{"user_id":"admin1","role":"admin","account_id":""}'

# User token (for account acc123)
curl -X POST http://localhost:8080/login \
  -H "Content-Type: application/json" \
  -d '{"user_id":"user1","role":"user","account_id":"acc123"}'
```

### 2. Create an account (admin only)

```bash
curl -X POST http://localhost:8080/api/accounts \
  -H "Authorization: Bearer <admin_token>" \
  -H "Content-Type: application/json" \
  -d '{"account_id":"acc123"}'
```

### 3. Deposit

```bash
curl -X POST http://localhost:8080/api/accounts/acc123/deposit \
  -H "Authorization: Bearer <user_token>" \
  -H "Content-Type: application/json" \
  -d '{"amount":500}'
```

### 4. Check balance

```bash
curl http://localhost:8080/api/accounts/acc123/balance \
  -H "Authorization: Bearer <user_token>"
```

### 5. Transfer

```bash
curl -X POST http://localhost:8080/api/transfer \
  -H "Authorization: Bearer <user_token>" \
  -H "Content-Type: application/json" \
  -d '{"from_account":"acc123","to_account":"acc456","amount":100}'
```

## Test Fault Tolerance

```bash
# Kill node2 (the leader might be on any node)
docker-compose stop node2

# The cluster should still work with node1 and node3
curl -X POST http://localhost:8080/api/accounts/acc123/deposit \
  -H "Authorization: Bearer <token>" \
  -d '{"amount":100}'

# Bring node2 back — it will catch up automatically
docker-compose start node2
```

## Key Concepts

| Concept | Where in code |
|---|---|
| Leader Election | `internal/raft/election.go → runCandidate()` |
| Log Replication | `internal/raft/rpc.go → HandleAppendEntries()` |
| Commit Majority | `internal/raft/election.go → maybeAdvanceCommitIndex()` |
| Apply to State | `cmd/node/main.go → applyCh goroutine` |
| Auth/Roles | `internal/auth/auth.go` |
| Banking Ops | `internal/bank/store.go` |
