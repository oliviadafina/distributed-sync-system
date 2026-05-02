import logging
import json
import time
from typing import Optional, List, Dict
import asyncio
from pydantic import BaseModel

import redis.asyncio as redis
from src.utils.config import settings
from src.utils.hash_ring import HashRing

logger = logging.getLogger(f"QueueManager-{settings.node_id}")

class Message(BaseModel):
    id: str
    topic: str
    payload: dict
    timestamp: float

class PublishRequest(BaseModel):
    topic: str
    payload: dict

class AckRequest(BaseModel):
    topic: str
    message_id: str

class QueueManager:
    def __init__(self):
        # Create a list of all nodes including self for consistent hashing
        all_nodes = [settings.node_id]
        # Peer URLs are like http://node2:8000. Let's extract node names or just use URLs.
        # To be consistent with Node IDs, we assume peers are passed in a way we can uniquely identify them.
        # For simplicity, we just hash the peer URLs + our own URL.
        self.my_url = f"http://{settings.node_host}:{settings.node_port}"
        # Let's override my_url to just the node_id so the ring is based on node_ids
        self.ring_nodes = [settings.node_id]
        
        # Parse peer node IDs from URLs (assuming format http://node2:8000)
        self.peer_urls = {}
        for peer_url in settings.peer_list:
            # Simple extraction: http://nodeX:8000 -> nodeX
            peer_id = peer_url.replace("http://", "").split(":")[0]
            self.ring_nodes.append(peer_id)
            self.peer_urls[peer_id] = peer_url
            
        self.hash_ring = HashRing(self.ring_nodes)
        
        # Connect to Redis
        self.redis = redis.Redis(
            host=settings.redis_host, 
            port=settings.redis_port,
            decode_responses=True
        )

    async def start(self):
        """Initializes Redis and starts recovery tasks."""
        logger.info(f"Connecting to Redis at {settings.redis_host}:{settings.redis_port}")
        # Start a background task to recover un-acked messages after a timeout
        asyncio.create_task(self._recover_stale_messages())

    async def _recover_stale_messages(self):
        """Background task to recover messages stuck in processing queues."""
        while True:
            try:
                # Find all processing queues
                keys = await self.redis.keys("queue:*:processing")
                for key in keys:
                    # key format: queue:topic_name:processing
                    topic = key.split(":")[1]
                    main_queue = f"queue:{topic}"
                    
                    # We check if there are items in the processing queue that have been there too long
                    # Since Redis lists don't store timestamps per element inherently, 
                    # we will just pop the oldest message and re-queue it if it's considered 'stale'.
                    # For a robust at-least-once, we could store metadata.
                    # Here we simply re-queue everything in processing list periodically (simple recovery).
                    
                    processing_msgs = await self.redis.lrange(key, 0, -1)
                    if processing_msgs:
                        logger.warning(f"Recovering {len(processing_msgs)} un-acked messages for topic {topic}")
                        for msg_str in processing_msgs:
                            # Re-queue to main queue
                            await self.redis.lpush(main_queue, msg_str)
                            # Remove from processing
                            await self.redis.lrem(key, 1, msg_str)
                            
            except Exception as e:
                logger.error(f"Error during message recovery: {e}")
                
            await asyncio.sleep(60) # Run recovery every 60 seconds

    def get_responsible_node(self, topic: str) -> str:
        """Determines which node is responsible for a topic based on consistent hashing."""
        return self.hash_ring.get_node(topic)

    async def enqueue(self, topic: str, payload: dict) -> Message:
        """Pushes a message to the tail of the queue."""
        message_id = f"msg_{time.time()}_{settings.node_id}"
        msg = Message(
            id=message_id,
            topic=topic,
            payload=payload,
            timestamp=time.time()
        )
        
        queue_key = f"queue:{topic}"
        await self.redis.lpush(queue_key, msg.json())
        logger.info(f"Enqueued message {message_id} to topic {topic}")
        return msg

    async def dequeue(self, topic: str, timeout: int = 5) -> Optional[Message]:
        """Pops a message from the queue and moves it to a processing list (at-least-once)."""
        main_queue = f"queue:{topic}"
        processing_queue = f"{main_queue}:processing"
        
        # BRPOPLPUSH blocks until a message is available or timeout
        # It pops from main_queue and pushes to processing_queue atomically
        result = await self.redis.brpoplpush(main_queue, processing_queue, timeout=timeout)
        
        if result:
            msg_data = json.loads(result)
            return Message(**msg_data)
        return None

    async def ack(self, topic: str, message_id: str) -> bool:
        """Acknowledges a message, removing it from the processing list."""
        processing_queue = f"queue:{topic}:processing"
        
        # We need to find the exact message string to remove it.
        # In a high-throughput system, we might use a hash map for O(1) removal,
        # but for this list-based approach, we iterate the processing list.
        processing_msgs = await self.redis.lrange(processing_queue, 0, -1)
        
        for msg_str in processing_msgs:
            msg_data = json.loads(msg_str)
            if msg_data["id"] == message_id:
                # Remove exactly 1 occurrence of this message string
                removed = await self.redis.lrem(processing_queue, 1, msg_str)
                if removed > 0:
                    logger.info(f"Acknowledged message {message_id} on topic {topic}")
                    return True
                    
        return False

queue_manager = QueueManager()
