import time
from graph import *
from pyspark import RDD
from pyspark.sql import SparkSession
from typing import Optional, Dict, Any, List

def calculate_contributions(node : tuple[int, tuple[Node, float]]) -> list[tuple[int, float]]:
    result = []

    for out_neighbor in node[1][0].out_neighbors:
        result.append((out_neighbor, node[1][1] / node[1][0].out_degree))

    return result

def rank_pages_unoptimized(
    nodes: RDD[Node],
    K: int,
    maxIter: int,
    epsilon: float,
    d: float = 0.85,
    track_timing: bool = False
) -> tuple[list[tuple[Node, float]], list[tuple[int, float]], Dict[str, Any]]:
    num_of_nodes = nodes.count()
    val = 1.0 / num_of_nodes

    initial = nodes.map(lambda x: (x.node_id, (x, val)))

    per_iter_times: list[float] = []
    total_start = time.perf_counter() if track_timing else None

    iteration = 0
    last_delta = None
    while iteration < maxIter:
        iter_start = time.perf_counter() if track_timing else None

        danglingMass = initial.filter(lambda x: x[1][0].out_degree == 0).map(lambda x: x[1][1]).sum()

        contributions = initial.flatMap(calculate_contributions).reduceByKey(lambda a, b: a + b)

        updated = initial.leftOuterJoin(contributions)

        combined = updated.mapValues(
            lambda x: (
                x[0][0],
                (1 - d) / num_of_nodes + d * (((x[1] if x[1] is not None else 0.0)) + (danglingMass / num_of_nodes))
            )
        )

        delta = initial.join(combined).map(lambda x: abs(x[1][1][1] - x[1][0][1])).sum()
        last_delta = delta

        initial = combined
        iteration += 1
        if track_timing and iter_start is not None:
            per_iter_times.append(time.perf_counter() - iter_start)
        if delta < epsilon:
            break

    total_time = (time.perf_counter() - total_start) if track_timing and total_start is not None else None

    top = initial.map(lambda x: (x[1][0], x[1][1])).top(K, key=lambda x: x[1])
    all_ranks = initial.map(lambda x: (x[0], x[1][1])).collect()

    stats = {
        "iterations": iteration,
        "last_delta": last_delta,
        "total_time": total_time,
        "per_iter_times": per_iter_times if track_timing else None,
    }

    return top, all_ranks, stats

def calculate_contributions_optimized(x: tuple[int, tuple[list[int], float]]) -> list[tuple[int, float]]:
    node_id, (neighbors, rank) = x
    result = []
    
    if neighbors:
        contribution_per_neighbor = rank / len(neighbors)
        for neighbor_id in neighbors:
            result.append((neighbor_id, contribution_per_neighbor))
    
    return result

def rank_pages_optimized(
    nodes: RDD[Node],
    K: int,
    maxIter: int,
    epsilon: float,
    d: float = 0.85,
    track_timing: bool = False,
    num_partitions: Optional[int] = None
) -> tuple[list[tuple[Node, float]], list[tuple[int, float]], Dict[str, Any]]:
    sc = nodes.context
    
    if num_partitions is None:
        num_partitions = max(sc.defaultParallelism * 2, 8)
    
    num_of_nodes = nodes.count()
    val = 1.0 / num_of_nodes

    graph = nodes.map(lambda n: (n.node_id, n.out_neighbors)).partitionBy(num_partitions).cache()
    
    node_map = nodes.map(lambda n: (n.node_id, n)).partitionBy(num_partitions).cache()

    ranks = graph.mapValues(lambda neighbors: val).cache()
    
    per_iter_times: list[float] = []
    total_start = time.perf_counter() if track_timing else None

    iteration = 0
    last_delta = None
    
    while iteration < maxIter:
        iter_start = time.perf_counter() if track_timing else None

        danglingMass = graph.join(ranks).filter(lambda x: len(x[1][0]) == 0).map(lambda x: x[1][1]).sum()

        contributions = graph.join(ranks).flatMap(calculate_contributions_optimized).reduceByKey(lambda a, b: a + b, numPartitions=num_partitions)

        node_ids = graph.mapValues(lambda x: None)
        new_ranks = node_ids.leftOuterJoin(contributions).mapValues(
                lambda x: (1 - d) / num_of_nodes + d * ((x[1] if x[1] is not None else 0.0) + danglingMass / num_of_nodes)
            ).partitionBy(num_partitions)
        
        delta = ranks.join(new_ranks).mapValues(lambda x: abs(x[0] - x[1])).values().sum()
        last_delta = delta

        ranks = new_ranks.cache()
   
        iteration += 1
        if track_timing and iter_start is not None:
            per_iter_times.append(time.perf_counter() - iter_start)
        if delta < epsilon:
            break

    total_time = (time.perf_counter() - total_start) if track_timing and total_start is not None else None

    final = node_map.join(ranks).map(lambda x: (x[1][0], x[1][1]))
    top = final.top(K, key=lambda x: x[1])
    all_ranks = ranks.collect()

    stats = {
        "iterations": iteration,
        "last_delta": last_delta,
        "total_time": total_time,
        "per_iter_times": per_iter_times if track_timing else None,
        "num_partitions": num_partitions,
    }

    return top, all_ranks, stats

def rank_pages_personalized(
    nodes: RDD[Node],
    seeds: List[int],
    K: int,
    maxIter: int,
    epsilon: float,
    d: float = 0.85,
    track_timing: bool = False,
    num_partitions: Optional[int] = None
) -> tuple[list[tuple[Node, float]], list[tuple[int, float]], Dict[str, Any]]:
    
    if not seeds:
        raise ValueError("Personalized variant requires seeds!")

    sc = nodes.context
    if num_partitions is None:
        num_partitions = max(sc.defaultParallelism * 2, 8)

    num_of_nodes = nodes.count()
    val = 1.0 / num_of_nodes

    graph = nodes.map(lambda n: (n.node_id, n.out_neighbors)).partitionBy(num_partitions).cache()
    node_map = nodes.map(lambda n: (n.node_id, n)).partitionBy(num_partitions).cache()

    ranks = graph.mapValues(lambda x : val).cache()

    seeds_set = set(seeds)
    num_of_seeds = len(seeds_set)
    broadcast_seeds = sc.broadcast(seeds_set)
    teleport_per_seed = (1.0 - d) / num_of_seeds

    node_ids = graph.mapValues(lambda _: None).cache()

    per_iter_times: list[float] = []
    total_start = time.perf_counter() if track_timing else None
    iteration = 0
    last_delta = None

    while iteration < maxIter:
        iter_start = time.perf_counter() if track_timing else None

        joined = graph.join(ranks).cache()

        danglingMass = joined.filter(lambda x: len(x[1][0]) == 0).map(lambda x: x[1][1]).sum()

        contributions = joined.flatMap(calculate_contributions_optimized).reduceByKey(lambda a, b: a + b, numPartitions=num_partitions)

        new_ranks = node_ids.leftOuterJoin(contributions) \
            .map(lambda x: (
                x[0], 
                (teleport_per_seed if x[0] in broadcast_seeds.value else 0.0)
                + d * ((x[1][1] if x[1][1] is not None else 0.0) + danglingMass / num_of_nodes))) \
            .partitionBy(num_partitions) \
            .cache()

        delta = ranks.join(new_ranks).mapValues(lambda x: abs(x[0] - x[1])).values().sum()
        last_delta = delta

        ranks = new_ranks

        iteration += 1
        if track_timing and iter_start is not None:
            per_iter_times.append(time.perf_counter() - iter_start)
        if delta < epsilon:
            break

    total_time = (time.perf_counter() - total_start) if track_timing and total_start is not None else None

    final = node_map.join(ranks).map(lambda x: (x[1][0], x[1][1]))
    top = final.top(K, key=lambda x: x[1])
    all_ranks = ranks.collect()

    stats = {
        "iterations": iteration,
        "last_delta": last_delta,
        "total_time": total_time,
        "per_iter_times": per_iter_times if track_timing else None,
        "num_partitions": num_partitions,
        "personalized": True,
        "seeds_count": num_of_seeds,
    }

    return top, all_ranks, stats

def validate(nodes_rdd: RDD[Node], ranks_rdd: RDD[tuple[int, float]], epsilon: float) -> tuple[bool, Dict[str, Any]]:
    details = {}
    
    #All ranks are non-negative
    negative_count = ranks_rdd.filter(lambda x: x[1] < 0.0).count()
    details["negative_ranks_count"] = negative_count
    if negative_count > 0:
        details["valid"] = False
        details["reason"] = "Found negative ranks"
        return False, details
    
    #Sum of ranks is approximately 1
    rank_sum = ranks_rdd.map(lambda x: x[1]).sum()
    details["rank_sum"] = rank_sum
    if abs(rank_sum - 1.0) > epsilon:
        details["valid"] = False
        details["reason"] = f"Rank sum {rank_sum} not close to 1.0 (epsilon={epsilon})"
        return False, details
    
    #Number of ranks matches number of nodes
    num_nodes = nodes_rdd.count()
    num_ranks = ranks_rdd.count()
    details["num_nodes"] = num_nodes
    details["num_ranks"] = num_ranks
    if num_nodes != num_ranks:
        details["valid"] = False
        details["reason"] = f"Mismatch: {num_nodes} nodes but {num_ranks} ranks"
        return False, details
    
    # Dangling nodes (outDegree=0) are properly treated
    # Verify that dangling nodes have ranks assigned
    dangling_nodes = nodes_rdd.filter(lambda n: n.out_degree == 0).map(lambda n: n.node_id)
    dangling_count = dangling_nodes.count()
    details["dangling_nodes_count"] = dangling_count
    
    if dangling_count > 0:
        dangling_ids_set = dangling_nodes.collect()
        ranks_map = ranks_rdd.collectAsMap()
        missing_dangling = [nid for nid in dangling_ids_set if nid not in ranks_map]
        details["missing_dangling_ranks"] = len(missing_dangling)
        
        if missing_dangling:
            details["valid"] = False
            details["reason"] = f"{len(missing_dangling)} dangling nodes missing ranks"
            return False, details
        
        dangling_ranks = [ranks_map[nid] for nid in dangling_ids_set]
        min_dangling_rank = min(dangling_ranks) if dangling_ranks else 0
        details["min_dangling_rank"] = min_dangling_rank
        
        if min_dangling_rank <= 0:
            details["valid"] = False
            details["reason"] = "Dangling nodes have non-positive ranks"
            return False, details
    
    details["valid"] = True
    return True, details

def main():
    spark = SparkSession.builder.appName("PageRank").master("local[*]").getOrCreate()

    sc = spark.sparkContext

    nodes = generate_synthetic_graph(num_nodes=20, num_edges=60, seed=42)
    rdd = sc.parallelize(nodes)

    top_k_unoptimized, all_ranks, stats = rank_pages_unoptimized(
        rdd, K=10, maxIter=20, epsilon=1e-3, d=0.85, track_timing=True
    )

    for node, rank in top_k_unoptimized:
        print(f"node={node.node_id} rank={rank:.6f} out_degree={node.out_degree}")

    if stats.get("total_time") is not None:
        print(f"Total time: {stats['total_time']:.4f}s over {stats['iterations']} iterations")
        print("Per-iteration times (s):" , ", ".join(f"{t:.4f}" for t in stats["per_iter_times"]))

    spark.stop()

if __name__ == "__main__":
    main()