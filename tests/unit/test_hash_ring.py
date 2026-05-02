import pytest
from src.utils.hash_ring import HashRing

def test_hash_ring_initialization():
    nodes = ["node1", "node2", "node3"]
    ring = HashRing(nodes, replicas=3)
    
    # Check if all virtual nodes are added (3 nodes * 3 replicas)
    assert len(ring.ring) == 9
    assert len(ring.sorted_keys) == 9

def test_hash_ring_distribution():
    nodes = ["node1", "node2", "node3"]
    ring = HashRing(nodes, replicas=10)
    
    # Test assigning 100 topics
    distribution = {"node1": 0, "node2": 0, "node3": 0}
    for i in range(100):
        target = ring.get_node(f"topic_{i}")
        distribution[target] += 1
        
    # Just ensure no node is completely left out in a 100-key distribution
    for count in distribution.values():
        assert count > 0

def test_hash_ring_add_remove():
    ring = HashRing(["node1"], replicas=1)
    
    # All keys should go to node1
    assert ring.get_node("any_key") == "node1"
    
    # Add node2
    ring.add_node("node2")
    assert len(ring.ring) == 2
    
    # Remove node1
    ring.remove_node("node1")
    assert ring.get_node("any_key") == "node2"
    assert len(ring.ring) == 1
