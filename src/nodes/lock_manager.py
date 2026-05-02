import asyncio
import logging
import time
from typing import Dict, List, Optional
from pydantic import BaseModel

from src.consensus.raft import raft_node, NodeState
from src.utils.config import settings

logger = logging.getLogger(f"LockManager-{settings.node_id}")

class LockRequest(BaseModel):
    resource: str
    client_id: str
    type: str = "exclusive"  # "exclusive" or "shared"
    timeout: int = 10  # seconds to wait for lock
    ttl: int = 30 # seconds until lock expires automatically

class LockState:
    def __init__(self):
        self.locks: Dict[str, Dict] = {}  # resource -> {"client_id": str, "type": str, "expires_at": float, "shared_clients": List[str]}
        self.wait_events: Dict[str, asyncio.Event] = {} # commit_index -> Event
        self.command_results: Dict[str, bool] = {} # commit_index -> success boolean
        
    def acquire(self, command: dict) -> bool:
        resource = command["resource"]
        client_id = command["client_id"]
        lock_type = command["type"]
        ttl = command.get("ttl", 30)
        
        now = time.time()
        
        # Cleanup expired lock
        if resource in self.locks:
            if self.locks[resource]["expires_at"] < now:
                del self.locks[resource]
                
        if resource not in self.locks:
            if lock_type == "exclusive":
                self.locks[resource] = {
                    "client_id": client_id,
                    "type": "exclusive",
                    "expires_at": now + ttl,
                    "shared_clients": []
                }
            else: # shared
                self.locks[resource] = {
                    "client_id": None,
                    "type": "shared",
                    "expires_at": now + ttl,
                    "shared_clients": [client_id]
                }
            return True
            
        current_lock = self.locks[resource]
        if current_lock["type"] == "exclusive":
            if current_lock["client_id"] == client_id:
                # Re-entrant or renew
                current_lock["expires_at"] = now + ttl
                return True
            return False
        else: # currently shared
            if lock_type == "shared":
                if client_id not in current_lock["shared_clients"]:
                    current_lock["shared_clients"].append(client_id)
                current_lock["expires_at"] = max(current_lock["expires_at"], now + ttl)
                return True
            return False

    def release(self, command: dict) -> bool:
        resource = command["resource"]
        client_id = command["client_id"]
        
        if resource not in self.locks:
            return False
            
        current_lock = self.locks[resource]
        if current_lock["type"] == "exclusive":
            if current_lock["client_id"] == client_id:
                del self.locks[resource]
                return True
            return False
        else:
            if client_id in current_lock["shared_clients"]:
                current_lock["shared_clients"].remove(client_id)
                if not current_lock["shared_clients"]:
                    del self.locks[resource]
                return True
            return False

class DistributedLockManager:
    def __init__(self):
        self.state = LockState()
        raft_node.set_apply_callback(self._on_log_applied)
        self._pending_commands: Dict[str, asyncio.Event] = {} # request_id -> Event
        self._command_results: Dict[str, bool] = {} # request_id -> bool
        
    async def _on_log_applied(self, index: int, command: dict):
        action = command.get("action")
        request_id = command.get("request_id")
        
        success = False
        if action == "acquire":
            success = self.state.acquire(command)
            logger.info(f"Applied ACQUIRE for {command['resource']} by {command['client_id']}: {success}")
        elif action == "release":
            success = self.state.release(command)
            logger.info(f"Applied RELEASE for {command['resource']} by {command['client_id']}: {success}")
            
        if request_id and request_id in self._pending_commands:
            self._command_results[request_id] = success
            self._pending_commands[request_id].set()

    async def acquire_lock(self, req: LockRequest) -> dict:
        if raft_node.state != NodeState.LEADER:
            # Tell client who the leader is (if known) or to retry
            leader = raft_node.voted_for if raft_node.voted_for else "unknown"
            # As an improvement, we could proxy the request to the leader here
            return {"status": "error", "message": f"Not leader. Try leader {leader}", "is_leader": False, "leader_id": leader}
            
        request_id = f"req_{time.time()}_{req.client_id}"
        command = {
            "action": "acquire",
            "request_id": request_id,
            "resource": req.resource,
            "client_id": req.client_id,
            "type": req.type,
            "ttl": req.ttl
        }
        
        event = asyncio.Event()
        self._pending_commands[request_id] = event
        
        # Append to Raft log
        success = await raft_node.append_command(command)
        if not success:
             del self._pending_commands[request_id]
             return {"status": "error", "message": "Failed to append to log (lost leadership?)"}
             
        # Wait for commit and apply
        try:
            await asyncio.wait_for(event.wait(), timeout=req.timeout)
            result = self._command_results.get(request_id, False)
            
            # Cleanup
            del self._pending_commands[request_id]
            if request_id in self._command_results:
                del self._command_results[request_id]
                
            if result:
                return {"status": "success", "message": "Lock acquired"}
            else:
                return {"status": "error", "message": "Lock currently held by another client"}
                
        except asyncio.TimeoutError:
            del self._pending_commands[request_id]
            return {"status": "error", "message": "Timeout waiting for lock to be committed"}

    async def release_lock(self, req: LockRequest) -> dict:
        if raft_node.state != NodeState.LEADER:
            leader = raft_node.voted_for if raft_node.voted_for else "unknown"
            return {"status": "error", "message": f"Not leader. Try leader {leader}", "is_leader": False, "leader_id": leader}
            
        request_id = f"req_{time.time()}_{req.client_id}"
        command = {
            "action": "release",
            "request_id": request_id,
            "resource": req.resource,
            "client_id": req.client_id
        }
        
        event = asyncio.Event()
        self._pending_commands[request_id] = event
        
        success = await raft_node.append_command(command)
        if not success:
             del self._pending_commands[request_id]
             return {"status": "error", "message": "Failed to append to log"}
             
        try:
            await asyncio.wait_for(event.wait(), timeout=5.0)
            result = self._command_results.get(request_id, False)
            
            del self._pending_commands[request_id]
            if request_id in self._command_results:
                del self._command_results[request_id]
                
            if result:
                return {"status": "success", "message": "Lock released"}
            else:
                return {"status": "error", "message": "Lock not held by client"}
                
        except asyncio.TimeoutError:
            del self._pending_commands[request_id]
            return {"status": "error", "message": "Timeout waiting for lock release to be committed"}

lock_manager = DistributedLockManager()
