import json
import random
from typing import List, Dict, Optional


class Node:
	def __init__(self, node_id: int, out_neighbors: Optional[List[int]] = None):
		self.node_id = node_id
		self.out_neighbors = out_neighbors if out_neighbors is not None else []
		self.out_degree = len(self.out_neighbors)
		self.income = 0.0

	def to_dict(self) -> Dict:
		return {"node_id": self.node_id, "out_neighbors": self.out_neighbors}

	@staticmethod
	def from_dict(data: Dict) -> "Node":
		return Node(int(data["node_id"]), list(data.get("out_neighbors", [])))


def generate_synthetic_graph(num_nodes: int, num_edges: int, seed: Optional[int] = None) -> List[Node]:
	if seed is not None:
		random.seed(seed)
	neighbors: List[List[int]] = [[] for _ in range(num_nodes)]
	edges = set()
	attempts = 0
	max_attempts = max(num_edges * 10, 1)
	while len(edges) < num_edges and attempts < max_attempts:
		src = random.randint(0, num_nodes - 1)
		dst = random.randint(0, num_nodes - 1)
		if src != dst and (src, dst) not in edges:
			edges.add((src, dst))
			neighbors[src].append(dst)
		attempts += 1
	return [Node(i, neighbors[i]) for i in range(num_nodes)]


def load_graph_from_edge_list(filepath: str, delimiter: str = "\t") -> List[Node]:
	edges = set()
	neighbors: Dict[int, List[int]] = {}
	with open(filepath, "r", encoding="utf-8") as f:
		for line in f:
			s = line.strip()
			if not s or s.startswith("#"):
				continue
			parts = s.split(delimiter)
			if len(parts) < 2:
				continue
			try:
				src = int(parts[0].strip())
				dst = int(parts[1].strip())
			except ValueError:
				continue
			if src == dst or (src, dst) in edges:
				continue
			edges.add((src, dst))
			neighbors.setdefault(src, []).append(dst)
			neighbors.setdefault(dst, neighbors.get(dst, []))
	ids = sorted(neighbors.keys())
	return [Node(nid, neighbors.get(nid, [])) for nid in ids]


def save_graph_to_edge_list(nodes: List[Node], filepath: str, delimiter: str = "\t") -> None:
	with open(filepath, "w", encoding="utf-8") as f:
		for n in nodes:
			for dst in n.out_neighbors:
				f.write(f"{n.node_id}{delimiter}{dst}\n")


def nodes_to_json(filepath: str, nodes: List[Node]) -> None:
	data = {"nodes": [n.to_dict() for n in nodes]}
	with open(filepath, "w", encoding="utf-8") as f:
		json.dump(data, f, indent=2)


def nodes_from_json(filepath: str) -> List[Node]:
	with open(filepath, "r", encoding="utf-8") as f:
		data = json.load(f)
	items = data.get("nodes", [])
	items.sort(key=lambda x: x["node_id"]) 
	return [Node.from_dict(x) for x in items]

