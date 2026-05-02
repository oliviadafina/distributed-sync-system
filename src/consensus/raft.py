import asyncio
import random
import logging
import time
from enum import Enum
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
import httpx

from src.utils.config import settings

logger = logging.getLogger(f"Raft-{settings.node_id}")

class NodeState(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

class LogEntry(BaseModel):
    term: int
    command: Any

class VoteRequest(BaseModel):
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int

class VoteResponse(BaseModel):
    term: int
    vote_granted: bool

class AppendEntriesRequest(BaseModel):
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry]
    leader_commit: int

class AppendEntriesResponse(BaseModel):
    term: int
    success: bool

class RaftNode:
    def __init__(self):
        self.node_id = settings.node_id
        self.peers = settings.peer_list
        
        # Seed random to avoid split-vote livelock across identical containers
        random.seed(self.node_id + str(time.time()))
        
        # Persistent state on all servers
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        
        # Volatile state on all servers
        self.commit_index = 0
        self.last_applied = 0
        self.state = NodeState.FOLLOWER
        
        # Volatile state on leaders
        self.next_index: Dict[str, int] = {peer: 1 for peer in self.peers}
        self.match_index: Dict[str, int] = {peer: 0 for peer in self.peers}
        
        # Timers
        self.election_timer: Optional[asyncio.Task] = None
        self.heartbeat_timer: Optional[asyncio.Task] = None
        
        self.http_client = None
        
        # Callback for state machine
        self.apply_callback = None
        
    def set_apply_callback(self, callback):
        self.apply_callback = callback
        
    def start(self):
        """Starts the Raft node operations."""
        self.http_client = httpx.AsyncClient(timeout=2.0)
        logger.info(f"Starting Raft node {self.node_id} as FOLLOWER")
        self._reset_election_timer()

    def stop(self):
        """Stops the Raft node timers."""
        if self.election_timer:
            self.election_timer.cancel()
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()

    def _get_election_timeout(self) -> float:
        """Returns a random election timeout in seconds."""
        return random.randint(settings.election_timeout_min, settings.election_timeout_max) / 1000.0

    def _reset_election_timer(self):
        """Resets the election timer."""
        if self.election_timer:
            self.election_timer.cancel()
        
        async def election_timeout_task():
            try:
                timeout = self._get_election_timeout()
                await asyncio.sleep(timeout)
                asyncio.create_task(self._start_election())
            except asyncio.CancelledError:
                pass
                
        self.election_timer = asyncio.create_task(election_timeout_task())

    async def _start_election(self):
        """Transitions to Candidate and starts an election."""
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        votes_received = 1  # Vote for self
        
        logger.info(f"Starting election for term {self.current_term}")
        
        self._reset_election_timer()
        
        last_log_index = len(self.log)
        last_log_term = self.log[-1].term if self.log else 0
        
        request = VoteRequest(
            term=self.current_term,
            candidate_id=self.node_id,
            last_log_index=last_log_index,
            last_log_term=last_log_term
        )

        # Request votes concurrently
        tasks = [self._send_vote_request(peer, request) for peer in self.peers]
        if tasks:
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            
            for response in responses:
                if isinstance(response, VoteResponse):
                    if response.term > self.current_term:
                        self.current_term = response.term
                        self.state = NodeState.FOLLOWER
                        self.voted_for = None
                        self._reset_election_timer()
                        return
                    if response.vote_granted:
                        votes_received += 1
        
        # Check if won election
        majority = (len(self.peers) + 1) // 2 + 1
        if self.state == NodeState.CANDIDATE and votes_received >= majority:
            self._become_leader()

    def _become_leader(self):
        """Transitions to Leader and starts sending heartbeats."""
        logger.info(f"Node {self.node_id} became LEADER for term {self.current_term}")
        self.state = NodeState.LEADER
        if self.election_timer:
            self.election_timer.cancel()
            
        # Reinitialize leader state
        for peer in self.peers:
            self.next_index[peer] = len(self.log) + 1
            self.match_index[peer] = 0
            
        self._start_heartbeats()

    def _start_heartbeats(self):
        """Starts sending periodic heartbeats."""
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
            
        async def heartbeat_task():
            try:
                while self.state == NodeState.LEADER:
                    await self._send_append_entries_to_all()
                    await asyncio.sleep(settings.heartbeat_interval / 1000.0)
            except asyncio.CancelledError:
                pass
                
        self.heartbeat_timer = asyncio.create_task(heartbeat_task())

    async def _send_vote_request(self, peer: str, request: VoteRequest) -> Optional[VoteResponse]:
        """Sends a vote request to a peer."""
        try:
            url = f"{peer}/raft/request_vote"
            logger.info(f"Sending vote request to {url}")
            response = await self.http_client.post(url, json=request.dict())
            if response.status_code == 200:
                return VoteResponse(**response.json())
        except Exception as e:
            logger.error(f"Failed to send vote request to {peer}: {e}")
        return None

    async def handle_vote_request(self, request: VoteRequest) -> VoteResponse:
        """Handles an incoming vote request."""
        if request.term > self.current_term:
            self.current_term = request.term
            self.state = NodeState.FOLLOWER
            self.voted_for = None
            self._reset_election_timer()

        vote_granted = False
        if request.term == self.current_term:
            last_log_index = len(self.log)
            last_log_term = self.log[-1].term if self.log else 0
            
            log_is_up_to_date = (request.last_log_term > last_log_term) or \
                                (request.last_log_term == last_log_term and request.last_log_index >= last_log_index)
                                
            if (self.voted_for is None or self.voted_for == request.candidate_id) and log_is_up_to_date:
                vote_granted = True
                self.voted_for = request.candidate_id
                self._reset_election_timer()
                
        return VoteResponse(term=self.current_term, vote_granted=vote_granted)

    async def _send_append_entries_to_all(self):
        """Sends AppendEntries (heartbeat/logs) to all peers."""
        tasks = []
        for peer in self.peers:
            prev_log_index = self.next_index[peer] - 1
            prev_log_term = self.log[prev_log_index - 1].term if prev_log_index > 0 else 0
            
            entries = self.log[prev_log_index:]
            
            request = AppendEntriesRequest(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries,
                leader_commit=self.commit_index
            )
            tasks.append(self._send_append_entries(peer, request))
            
        if tasks:
            await asyncio.gather(*tasks)

    async def _send_append_entries(self, peer: str, request: AppendEntriesRequest):
        """Sends an AppendEntries request to a peer."""
        try:
            url = f"{peer}/raft/append_entries"
            response = await self.http_client.post(url, json=request.dict())
            if response.status_code == 200:
                resp = AppendEntriesResponse(**response.json())
                
                if resp.term > self.current_term:
                    self.current_term = resp.term
                    self.state = NodeState.FOLLOWER
                    self.voted_for = None
                    self._reset_election_timer()
                    if self.heartbeat_timer:
                        self.heartbeat_timer.cancel()
                    return

                if self.state == NodeState.LEADER:
                    if resp.success:
                        self.next_index[peer] = request.prev_log_index + len(request.entries) + 1
                        self.match_index[peer] = self.next_index[peer] - 1
                        self._update_commit_index()
                    else:
                        self.next_index[peer] = max(1, self.next_index[peer] - 1)
                        
        except Exception as e:
            logger.debug(f"Failed to send append entries to {peer}: {e}")

    def _update_commit_index(self):
        """Updates the commit index based on match_index of majority."""
        for n in range(len(self.log), self.commit_index, -1):
            if self.log[n-1].term == self.current_term:
                match_count = 1  # Self
                for peer in self.peers:
                    if self.match_index.get(peer, 0) >= n:
                        match_count += 1
                
                majority = (len(self.peers) + 1) // 2 + 1
                if match_count >= majority:
                    self.commit_index = n
                    self._apply_logs()
                    break

    def _apply_logs(self):
        """Applies committed logs to the state machine (to be implemented by subclasses or lock manager)."""
        while self.commit_index > self.last_applied:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            logger.info(f"Applying log entry {self.last_applied}: {entry.command}")
            if self.apply_callback:
                asyncio.create_task(self.apply_callback(self.last_applied, entry.command))

    async def handle_append_entries(self, request: AppendEntriesRequest) -> AppendEntriesResponse:
        """Handles incoming AppendEntries request."""
        if request.term < self.current_term:
            return AppendEntriesResponse(term=self.current_term, success=False)

        if request.term > self.current_term:
            self.current_term = request.term
            self.voted_for = None
            
        self.state = NodeState.FOLLOWER
        self._reset_election_timer()
        
        # Check log matching property
        if request.prev_log_index > len(self.log):
            return AppendEntriesResponse(term=self.current_term, success=False)
            
        if request.prev_log_index > 0 and self.log[request.prev_log_index - 1].term != request.prev_log_term:
            # Delete conflict log and all that follow
            self.log = self.log[:request.prev_log_index - 1]
            return AppendEntriesResponse(term=self.current_term, success=False)
            
        # Append any new entries not already in the log
        for i, entry in enumerate(request.entries):
            index = request.prev_log_index + 1 + i
            if index > len(self.log):
                self.log.append(entry)
            elif self.log[index - 1].term != entry.term:
                self.log = self.log[:index - 1]
                self.log.append(entry)
                
        if request.leader_commit > self.commit_index:
            self.commit_index = min(request.leader_commit, len(self.log))
            self._apply_logs()
            
        return AppendEntriesResponse(term=self.current_term, success=True)
        
    async def append_command(self, command: Any) -> bool:
        """Appends a command to the log if this node is the leader."""
        if self.state != NodeState.LEADER:
            return False
            
        entry = LogEntry(term=self.current_term, command=command)
        self.log.append(entry)
        
        # Trigger immediate heartbeat to replicate
        await self._send_append_entries_to_all()
        return True

# Global instance for the node
raft_node = RaftNode()
