import hashlib
import bisect
from typing import List

class HashRing:
    def __init__(self, nodes: List[str], replicas: int = 3):
        """
        Initializes the HashRing for consistent hashing.
        :param nodes: List of node IDs/URLs.
        :param replicas: Number of virtual nodes per actual node to ensure even distribution.
        """
        self.replicas = replicas
        self.ring = {}
        self.sorted_keys = []
        
        for node in nodes:
            self.add_node(node)
            
    def _hash(self, key: str) -> int:
        """Generates a simple MD5 hash converted to an integer."""
        m = hashlib.md5()
        m.update(key.encode('utf-8'))
        return int(m.hexdigest(), 16)

    def add_node(self, node: str):
        """Adds a node to the hash ring."""
        for i in range(self.replicas):
            virtual_node_id = f"{node}:{i}"
            key = self._hash(virtual_node_id)
            self.ring[key] = node
            bisect.insort(self.sorted_keys, key)

    def remove_node(self, node: str):
        """Removes a node from the hash ring."""
        for i in range(self.replicas):
            virtual_node_id = f"{node}:{i}"
            key = self._hash(virtual_node_id)
            if key in self.ring:
                del self.ring[key]
                self.sorted_keys.remove(key)

    def get_node(self, string_key: str) -> str:
        """Returns the node responsible for the given key (e.g., topic name)."""
        if not self.ring:
            return None
            
        hash_val = self._hash(string_key)
        
        # Find the first key in the ring that is >= hash_val
        idx = bisect.bisect_right(self.sorted_keys, hash_val)
        
        if idx == len(self.sorted_keys):
            # Wrap around to the first node
            idx = 0
            
        return self.ring[self.sorted_keys[idx]]
