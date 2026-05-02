# System Architecture: Distributed Synchronization System

## Overview
This document describes the architecture of our Distributed Synchronization System. The system consists of three main distributed components running simultaneously on multiple nodes (simulated via Docker containers):
1. **Distributed Lock Manager** (Powered by Raft Consensus)
2. **Distributed Queue System** (Powered by Consistent Hashing & Redis)
3. **Distributed Cache Coherence** (Powered by MESI Protocol & LRU)

## Node Architecture
Each node runs a **FastAPI** web server, acting as both the public interface for clients and the internal communication interface for inter-node protocols (Raft, Bus snooping, Queue routing).

```mermaid
graph TD
    Client((Client)) --> Node1
    Client --> Node2
    Client --> Node3
    
    subgraph "Distributed Cluster"
        Node1[Node 1 (FastAPI)]
        Node2[Node 2 (FastAPI)]
        Node3[Node 3 (FastAPI)]
        
        Redis[(Redis Shared State)]
        
        Node1 <--> Node2
        Node2 <--> Node3
        Node1 <--> Node3
        
        Node1 -.-> Redis
        Node2 -.-> Redis
        Node3 -.-> Redis
    end
```

## 1. Distributed Lock Manager (Raft Consensus)
The lock manager uses a custom implementation of the Raft Consensus Algorithm to maintain a consistent state of locks across the cluster.

### Mechanism
- **Leader Election:** If a leader fails, nodes transition to candidates and elect a new leader using randomized timeouts.
- **Log Replication:** All lock requests (`ACQUIRE`, `RELEASE`) must go through the leader. The leader appends the command to its log and replicates it to followers via `AppendEntries`.
- **Commit & Apply:** Once a majority of nodes acknowledge the log, it is committed. The `DistributedLockManager` listens to the commit stream and updates the lock states simultaneously across all machines.

## 2. Distributed Queue System (Consistent Hashing)
The queue system allows producers and consumers to push and pop messages seamlessly, using a hash ring to assign topics to specific nodes.

### Mechanism
- **Hash Ring:** `HashRing` assigns virtual nodes to the ring. Topics are hashed (MD5) to find the responsible node.
- **Routing:** If a client requests `Node A` for `topic_X`, but `Node C` is responsible, `Node A` automatically forwards the HTTP request to `Node C`.
- **At-Least-Once Delivery:** Messages are pushed to a Redis List. When dequeued, they are atomically moved to a `processing` list (using `BRPOPLPUSH`). They remain there until an explicit `ACK` is received. If a client crashes, a background task recovers stale messages.

## 3. Distributed Cache Coherence (MESI)
Each node maintains a local in-memory cache with an LRU replacement policy. Redis acts as the "Main Memory".

### Protocol States
- **M (Modified):** Data is modified locally. Not synced to Redis.
- **E (Exclusive):** Data is identical to Redis. Only this node has it.
- **S (Shared):** Data is identical to Redis. Other nodes might have it.
- **I (Invalid):** Data is stale and cannot be used.

### Mechanism
- **Reads:** If Miss, broadcasts `BusRd`. Other nodes snoop; if they have `M`, they flush to Redis and transition to `S`.
- **Writes:** If Miss or `S`, broadcasts `BusRdX` or `BusUpgr` to invalidate other copies. Transitions to `M`.
- **Eviction:** When the cache reaches its limit, the Least Recently Used item is evicted. If it was `M`, it is flushed to Redis first.
