from pydantic_settings import BaseSettings
from typing import List

class Settings(BaseSettings):
    node_id: str = "node_1"
    node_host: str = "0.0.0.0"
    node_port: int = 8000
    
    peers: str = "" # Comma separated peer URLs
    
    redis_host: str = "redis"
    redis_port: int = 6379
    
    election_timeout_min: int = 1500
    election_timeout_max: int = 3000
    heartbeat_interval: int = 500

    @property
    def peer_list(self) -> List[str]:
        if not self.peers:
            return []
        return [peer.strip() for peer in self.peers.split(",") if peer.strip()]

    class Config:
        env_file = ".env"

settings = Settings()
