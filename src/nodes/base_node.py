from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import logging
from contextlib import asynccontextmanager

from src.utils.config import settings
from src.consensus.raft import raft_node, VoteRequest, AppendEntriesRequest
from src.nodes.lock_manager import lock_manager, LockRequest
from src.nodes.queue_node import queue_manager, PublishRequest, AckRequest
from src.nodes.cache_node import cache_manager, BusMessage
from src.utils.security import get_current_role, require_admin, audit_log
from fastapi import Depends, HTTPException
import httpx
from pydantic import BaseModel

class CacheWriteRequest(BaseModel):
    value: str

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
)
logger = logging.getLogger(settings.node_id)

http_client = httpx.AsyncClient(timeout=5.0)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Initializing Node...")
    raft_node.start()
    await queue_manager.start()
    yield
    # Shutdown
    logger.info("Shutting down Node...")
    raft_node.stop()
    await http_client.aclose()
    await cache_manager.http_client.aclose()

app = FastAPI(
    title=f"Distributed Sync Node - {settings.node_id}",
    description="A node in the distributed synchronization system.",
    lifespan=lifespan
)

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "ok", 
        "node_id": settings.node_id,
        "raft_state": raft_node.state,
        "current_term": raft_node.current_term,
        "voted_for": raft_node.voted_for,
        "commit_index": raft_node.commit_index,
        "peers": raft_node.peers
    }

@app.post("/raft/request_vote")
async def request_vote(request: VoteRequest):
    return await raft_node.handle_vote_request(request)

@app.post("/raft/append_entries")
async def append_entries(request: AppendEntriesRequest):
    return await raft_node.handle_append_entries(request)

@app.post("/lock/acquire")
async def api_acquire_lock(req: LockRequest, role: str = Depends(get_current_role)):
    if req.type == "exclusive" and role != "admin":
        raise HTTPException(status_code=403, detail="Hanya role 'admin' yang bisa menggunakan exclusive lock")
    audit_log("ACQUIRE_LOCK", req.resource, f"{req.client_id} (Role: {role})")
    return await lock_manager.acquire_lock(req)

@app.post("/lock/release")
async def api_release_lock(req: LockRequest, role: str = Depends(get_current_role)):
    audit_log("RELEASE_LOCK", req.resource, f"{req.client_id} (Role: {role})")
    return await lock_manager.release_lock(req)

async def forward_request(node_id: str, path: str, payload: dict, headers: dict = None):
    """Forwards a request to the responsible node."""
    if node_id not in queue_manager.peer_urls:
        return JSONResponse(status_code=500, content={"error": f"Unknown node {node_id}"})
    
    target_url = f"{queue_manager.peer_urls[node_id]}{path}"
    try:
        response = await http_client.post(target_url, json=payload, headers=headers)
        return JSONResponse(status_code=response.status_code, content=response.json())
    except Exception as e:
        logger.error(f"Failed to forward request to {target_url}: {e}")
        return JSONResponse(status_code=503, content={"error": "Node unreachable"})

@app.post("/queue/publish")
async def api_publish(req: PublishRequest, role: str = Depends(require_admin)):
    audit_log("PUBLISH_QUEUE", req.topic, f"Role: {role}")
    responsible_node = queue_manager.get_responsible_node(req.topic)
    
    if responsible_node != settings.node_id:
        logger.info(f"Forwarding publish for {req.topic} to {responsible_node}")
        # Meneruskan API Key agar node tujuan juga memberikan izin
        headers = {"X-API-Key": "admin-secret-key-123"}
        return await forward_request(responsible_node, "/queue/publish", req.dict(), headers=headers)
        
    msg = await queue_manager.enqueue(req.topic, req.payload)
    return {"status": "success", "message": msg.dict()}

@app.get("/queue/poll/{topic}")
async def api_poll(topic: str, timeout: int = 5, role: str = Depends(get_current_role)):
    audit_log("POLL_QUEUE", topic, f"Role: {role}")
    responsible_node = queue_manager.get_responsible_node(topic)
    
    if responsible_node != settings.node_id:
        logger.info(f"Forwarding poll for {topic} to {responsible_node}")
        if responsible_node not in queue_manager.peer_urls:
            return JSONResponse(status_code=500, content={"error": f"Unknown node {responsible_node}"})
        
        target_url = f"{queue_manager.peer_urls[responsible_node]}/queue/poll/{topic}?timeout={timeout}"
        try:
            # Forward the API Key
            headers = {"X-API-Key": "admin-secret-key-123"} if role == "admin" else {"X-API-Key": "viewer-secret-key-456"}
            response = await http_client.get(target_url, headers=headers)
            return JSONResponse(status_code=response.status_code, content=response.json())
        except Exception as e:
            logger.error(f"Failed to forward poll request to {target_url}: {e}")
            return JSONResponse(status_code=503, content={"error": "Node unreachable"})
            
    msg = await queue_manager.dequeue(topic, timeout)
    if msg:
        return {"status": "success", "message": msg.dict()}
    return {"status": "empty", "message": None}

@app.post("/queue/ack")
async def api_ack(req: AckRequest, role: str = Depends(get_current_role)):
    audit_log("ACK_QUEUE", req.topic, f"Role: {role}")
    responsible_node = queue_manager.get_responsible_node(req.topic)
    
    if responsible_node != settings.node_id:
        logger.info(f"Forwarding ack for {req.topic} to {responsible_node}")
        headers = {"X-API-Key": "admin-secret-key-123"} if role == "admin" else {"X-API-Key": "viewer-secret-key-456"}
        return await forward_request(responsible_node, "/queue/ack", req.dict(), headers=headers)
        
    success = await queue_manager.ack(req.topic, req.message_id)
    if success:
        return {"status": "success"}
    return JSONResponse(status_code=404, content={"error": "Message not found or already acknowledged"})

@app.post("/cache/snoop")
async def api_cache_snoop(msg: BusMessage):
    return await cache_manager.handle_snoop(msg)

@app.get("/cache/metrics")
async def api_cache_metrics():
    return cache_manager.get_metrics()

@app.get("/cache/{key}")
async def api_cache_read(key: str, role: str = Depends(get_current_role)):
    audit_log("READ_CACHE", key, f"Role: {role}")
    val = await cache_manager.read(key)
    if val is not None:
        return {"status": "success", "key": key, "value": val}
    return JSONResponse(status_code=404, content={"error": "Key not found in cache or memory"})

@app.post("/cache/{key}")
async def api_cache_write(key: str, req: CacheWriteRequest, role: str = Depends(require_admin)):
    audit_log("WRITE_CACHE", key, f"Role: {role}")
    await cache_manager.write(key, req.value)
    return {"status": "success", "message": f"Written to {key}"}

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Global exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"message": "Internal server error", "details": str(exc)}
    )

