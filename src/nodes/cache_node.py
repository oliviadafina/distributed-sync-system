import logging
import time
from enum import Enum
from typing import Dict, Any, Optional
import httpx
from collections import OrderedDict
from pydantic import BaseModel
import asyncio

from src.utils.config import settings
import redis.asyncio as redis

logger = logging.getLogger(f"CacheNode-{settings.node_id}")

class CacheState(str, Enum):
    MODIFIED = "M"
    EXCLUSIVE = "E"
    SHARED = "S"
    INVALID = "I"

class CacheEntry:
    def __init__(self, key: str, value: Any, state: CacheState):
        self.key = key
        self.value = value
        self.state = state
        self.last_accessed = time.time()

class BusMessage(BaseModel):
    type: str # "BusRd", "BusRdX", "BusUpgr", "Flush"
    key: str
    value: Optional[Any] = None
    sender_id: str

class CacheManager:
    def __init__(self, capacity: int = 100):
        self.capacity = capacity
        # OrderedDict for LRU: keys accessed recently are moved to the end.
        self.cache: OrderedDict[str, CacheEntry] = OrderedDict()
        
        # Redis acts as the "Main Memory"
        self.redis = redis.Redis(
            host=settings.redis_host, 
            port=settings.redis_port,
            decode_responses=True
        )
        
        self.peer_urls = {}
        for peer_url in settings.peer_list:
            peer_id = peer_url.replace("http://", "").split(":")[0]
            self.peer_urls[peer_id] = peer_url
            
        self.http_client = httpx.AsyncClient(timeout=2.0)
        
        # Metrics collection
        self.metrics = {
            "hits": 0,
            "misses": 0,
            "evictions": 0,
            "bus_messages_sent": 0
        }

    async def _evict_if_needed(self):
        """LRU Eviction Policy."""
        if len(self.cache) >= self.capacity:
            # Pop the first item (least recently used)
            key, entry = self.cache.popitem(last=False)
            self.metrics["evictions"] += 1
            logger.info(f"Evicting cache line: {key} (State: {entry.state})")
            
            # If modified, we must write back to memory
            if entry.state == CacheState.MODIFIED:
                await self._write_to_memory(key, entry.value)
                
    async def _read_from_memory(self, key: str) -> Optional[Any]:
        val = await self.redis.get(f"memory:{key}")
        return val

    async def _write_to_memory(self, key: str, value: Any):
        await self.redis.set(f"memory:{key}", str(value))

    async def _broadcast_bus_message(self, msg_type: str, key: str, value: Any = None) -> bool:
        """Broadcasts a message to all peers on the 'bus'."""
        self.metrics["bus_messages_sent"] += 1
        msg = BusMessage(type=msg_type, key=key, value=value, sender_id=settings.node_id)
        
        tasks = []
        for peer_id, url in self.peer_urls.items():
            target_url = f"{url}/cache/snoop"
            tasks.append(self.http_client.post(target_url, json=msg.dict()))
            
        if tasks:
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            # Check if any peer responded indicating they have the line shared
            for resp in responses:
                if not isinstance(resp, Exception) and resp.status_code == 200:
                    data = resp.json()
                    if data.get("has_copy"):
                        return True # Someone else has a copy
        return False

    async def handle_snoop(self, msg: BusMessage) -> dict:
        """Snoops on the bus to maintain coherence."""
        key = msg.key
        response = {"has_copy": False}
        
        if key in self.cache:
            entry = self.cache[key]
            
            if msg.type == "BusRd":
                # Another cache wants to read
                if entry.state == CacheState.MODIFIED:
                    # Flush to memory, downgrade to S
                    await self._write_to_memory(key, entry.value)
                    entry.state = CacheState.SHARED
                    response["has_copy"] = True
                elif entry.state in [CacheState.EXCLUSIVE, CacheState.SHARED]:
                    # Downgrade E to S, S stays S
                    entry.state = CacheState.SHARED
                    response["has_copy"] = True
                    
            elif msg.type == "BusRdX":
                # Another cache wants to write and misses
                if entry.state == CacheState.MODIFIED:
                    # Flush to memory, invalidate
                    await self._write_to_memory(key, entry.value)
                # Invalidate our copy
                entry.state = CacheState.INVALID
                del self.cache[key]
                
            elif msg.type == "BusUpgr":
                # Another cache wants to write and had S
                entry.state = CacheState.INVALID
                if key in self.cache:
                    del self.cache[key]
                    
        return response

    async def read(self, key: str) -> Optional[Any]:
        """Reads a value from cache, transitioning states according to MESI."""
        if key in self.cache:
            entry = self.cache[key]
            if entry.state != CacheState.INVALID:
                self.metrics["hits"] += 1
                self.cache.move_to_end(key) # Mark as recently used
                return entry.value

        self.metrics["misses"] += 1
        await self._evict_if_needed()
        
        # Cache Miss - Broadcast BusRd
        has_shared_copy = await self._broadcast_bus_message("BusRd", key)
        
        # Read from Memory
        val = await self._read_from_memory(key)
        
        if val is not None:
            new_state = CacheState.SHARED if has_shared_copy else CacheState.EXCLUSIVE
            self.cache[key] = CacheEntry(key, val, new_state)
            return val
            
        return None

    async def write(self, key: str, value: Any):
        """Writes a value to cache, transitioning states according to MESI."""
        if key in self.cache:
            entry = self.cache[key]
            self.cache.move_to_end(key) # Mark as recently used
            
            if entry.state in [CacheState.MODIFIED, CacheState.EXCLUSIVE]:
                # We already have write permission
                entry.value = value
                entry.state = CacheState.MODIFIED
                self.metrics["hits"] += 1
                return
                
            elif entry.state == CacheState.SHARED:
                # We need to invalidate others
                await self._broadcast_bus_message("BusUpgr", key)
                entry.value = value
                entry.state = CacheState.MODIFIED
                self.metrics["hits"] += 1
                return

        # Cache Miss or Invalid
        self.metrics["misses"] += 1
        await self._evict_if_needed()
        
        # Broadcast BusRdX
        await self._broadcast_bus_message("BusRdX", key)
        
        # Update local
        self.cache[key] = CacheEntry(key, value, CacheState.MODIFIED)

    def get_metrics(self) -> dict:
        return {
            "node_id": settings.node_id,
            "cache_size": len(self.cache),
            "capacity": self.capacity,
            "metrics": self.metrics,
            "cache_state": {k: v.state.value for k, v in self.cache.items()}
        }

cache_manager = CacheManager(capacity=100)
