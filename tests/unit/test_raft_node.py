"""
Unit Tests for RaftNode (Consensus Algorithm)
Tests Raft state machine logic — no real network or asyncio event loops needed
for the pure-logic parts; async methods use pytest-asyncio.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# ─── Fixture ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def raft():
    """Returns a fresh RaftNode with settings and httpx mocked."""
    with patch("src.consensus.raft.settings") as mock_settings, \
         patch("src.consensus.raft.httpx.AsyncClient"):

        mock_settings.node_id = "node_test"
        mock_settings.peer_list = ["http://node2:8002", "http://node3:8003"]
        mock_settings.election_timeout_min = 150
        mock_settings.election_timeout_max = 300
        mock_settings.heartbeat_interval = 50

        from src.consensus.raft import RaftNode, NodeState, LogEntry, VoteRequest, AppendEntriesRequest
        node = RaftNode()
        node.http_client = AsyncMock()
        return node, NodeState, LogEntry, VoteRequest, AppendEntriesRequest


# ── Initial State Tests ───────────────────────────────────────────────────────

class TestRaftInitialState:
    def test_starts_as_follower(self, raft):
        node, NodeState, *_ = raft
        assert node.state == NodeState.FOLLOWER

    def test_initial_term_is_zero(self, raft):
        node, *_ = raft
        assert node.current_term == 0

    def test_initial_log_is_empty(self, raft):
        node, *_ = raft
        assert node.log == []

    def test_initial_commit_index_is_zero(self, raft):
        node, *_ = raft
        assert node.commit_index == 0


# ── Vote Request Handling ─────────────────────────────────────────────────────

class TestHandleVoteRequest:
    @pytest.mark.asyncio
    async def test_grants_vote_for_higher_term_candidate(self, raft):
        """Node should grant vote to a candidate with a higher term."""
        node, NodeState, LogEntry, VoteRequest, _ = raft
        req = VoteRequest(term=1, candidate_id="node2", last_log_index=0, last_log_term=0)
        response = await node.handle_vote_request(req)
        assert response.vote_granted is True
        assert node.voted_for == "node2"

    @pytest.mark.asyncio
    async def test_rejects_vote_for_lower_term(self, raft):
        """Node should reject vote requests with a term lower than current."""
        node, NodeState, LogEntry, VoteRequest, _ = raft
        node.current_term = 5
        req = VoteRequest(term=3, candidate_id="node2", last_log_index=0, last_log_term=0)
        response = await node.handle_vote_request(req)
        assert response.vote_granted is False

    @pytest.mark.asyncio
    async def test_does_not_double_vote_in_same_term(self, raft):
        """Node should not grant vote to a second candidate in the same term."""
        node, NodeState, LogEntry, VoteRequest, _ = raft
        req1 = VoteRequest(term=1, candidate_id="node2", last_log_index=0, last_log_term=0)
        req2 = VoteRequest(term=1, candidate_id="node3", last_log_index=0, last_log_term=0)
        await node.handle_vote_request(req1)
        response = await node.handle_vote_request(req2)
        assert response.vote_granted is False

    @pytest.mark.asyncio
    async def test_revotes_for_same_candidate(self, raft):
        """Node can re-vote for the same candidate it already voted for."""
        node, NodeState, LogEntry, VoteRequest, _ = raft
        req = VoteRequest(term=1, candidate_id="node2", last_log_index=0, last_log_term=0)
        await node.handle_vote_request(req)
        response = await node.handle_vote_request(req)
        assert response.vote_granted is True

    @pytest.mark.asyncio
    async def test_updates_term_on_higher_term_request(self, raft):
        """Receiving a higher term must update the node's current term."""
        node, NodeState, LogEntry, VoteRequest, _ = raft
        node.current_term = 2
        req = VoteRequest(term=10, candidate_id="node2", last_log_index=0, last_log_term=0)
        await node.handle_vote_request(req)
        assert node.current_term == 10


# ── AppendEntries Handling ────────────────────────────────────────────────────

class TestHandleAppendEntries:
    @pytest.mark.asyncio
    async def test_rejects_stale_term_heartbeat(self, raft):
        """AppendEntries from a leader with stale term must be rejected."""
        node, NodeState, LogEntry, _, AppendEntriesRequest = raft
        node.current_term = 5
        req = AppendEntriesRequest(
            term=3, leader_id="node2",
            prev_log_index=0, prev_log_term=0,
            entries=[], leader_commit=0
        )
        resp = await node.handle_append_entries(req)
        assert resp.success is False

    @pytest.mark.asyncio
    async def test_accepts_valid_heartbeat(self, raft):
        """Valid heartbeat (empty entries, correct term) must succeed."""
        node, NodeState, LogEntry, _, AppendEntriesRequest = raft
        req = AppendEntriesRequest(
            term=1, leader_id="node2",
            prev_log_index=0, prev_log_term=0,
            entries=[], leader_commit=0
        )
        resp = await node.handle_append_entries(req)
        assert resp.success is True
        assert node.state == NodeState.FOLLOWER

    @pytest.mark.asyncio
    async def test_appends_new_log_entries(self, raft):
        """Leader's new log entries should be appended to follower's log."""
        node, NodeState, LogEntry, _, AppendEntriesRequest = raft
        entries = [LogEntry(term=1, command={"action": "acquire"})]
        req = AppendEntriesRequest(
            term=1, leader_id="node2",
            prev_log_index=0, prev_log_term=0,
            entries=entries, leader_commit=0
        )
        resp = await node.handle_append_entries(req)
        assert resp.success is True
        assert len(node.log) == 1

    @pytest.mark.asyncio
    async def test_updates_commit_index_from_leader(self, raft):
        """Follower should update commit_index based on leader_commit."""
        node, NodeState, LogEntry, _, AppendEntriesRequest = raft
        entries = [LogEntry(term=1, command={"action": "acquire"})]
        req = AppendEntriesRequest(
            term=1, leader_id="node2",
            prev_log_index=0, prev_log_term=0,
            entries=entries, leader_commit=1
        )
        await node.handle_append_entries(req)
        assert node.commit_index == 1

    @pytest.mark.asyncio
    async def test_reverts_to_follower_on_higher_term_append(self, raft):
        """A candidate or leader must revert to follower upon higher-term AppendEntries."""
        node, NodeState, LogEntry, _, AppendEntriesRequest = raft
        node.state = NodeState.CANDIDATE
        node.current_term = 1
        req = AppendEntriesRequest(
            term=2, leader_id="node2",
            prev_log_index=0, prev_log_term=0,
            entries=[], leader_commit=0
        )
        await node.handle_append_entries(req)
        assert node.state == NodeState.FOLLOWER
        assert node.current_term == 2


# ── append_command Tests ──────────────────────────────────────────────────────

class TestAppendCommand:
    @pytest.mark.asyncio
    async def test_append_command_fails_if_not_leader(self, raft):
        """Non-leader node must reject append_command."""
        node, NodeState, *_ = raft
        node.state = NodeState.FOLLOWER
        result = await node.append_command({"action": "acquire"})
        assert result is False
        assert len(node.log) == 0

    @pytest.mark.asyncio
    async def test_append_command_succeeds_as_leader(self, raft):
        """Leader node should successfully append command to log."""
        node, NodeState, *_ = raft
        node.state = NodeState.LEADER
        node._send_append_entries_to_all = AsyncMock()
        result = await node.append_command({"action": "acquire", "resource": "db"})
        assert result is True
        assert len(node.log) == 1
        assert node.log[0].command["action"] == "acquire"
