import argparse
import json
import random
from typing import List, Tuple
from graph import load_graph_from_edge_list, generate_synthetic_graph, nodes_to_json
from pagerank import rank_pages_unoptimized, rank_pages_optimized, rank_pages_personalized, validate, Node
from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PageRank CLI")
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--edge-list", type=str, help="Path to edge-list file")
    src.add_argument("--generate", action="store_true", help="Generate synthetic graph")
    parser.add_argument("--num-nodes", type=int, default=1000)
    parser.add_argument("--num-edges", type=int, default=4000)
    parser.add_argument("--seed", type=int, default=42, help="Random seed for generation")

    # Output
    parser.add_argument("--output-graph", type=str, help="Optional: save generated graph to edge-list")
    parser.add_argument("--output-ranks", type=str, help="Write final ranks as TSV (nodeId<TAB>rank)")
    parser.add_argument("--output-json", type=str, help="Write comparison results to JSON (for --compare)")

    # Algorithm params
    parser.add_argument("--d", type=float, default=0.85)
    parser.add_argument("--max-iter", type=int, default=20)
    parser.add_argument("--epsilon", type=float, default=1e-3)
    parser.add_argument("--K", type=int, default=20)
    parser.add_argument("--num-partitions", type=int, help="Partitions for optimized")

    # Mode
    parser.add_argument("--optimized", action="store_true", help="Run optimized")
    parser.add_argument("--personalized", action="store_true", help="Run Personalized PageRank")
    parser.add_argument("--compare", action="store_true", help="Compare standard vs personalized over multiple runs")
    parser.add_argument("--runs", type=int, default=10, help="Number of random graphs to generate for comparison")
    parser.add_argument("--seeds-per-run", type=int, default=1, help="Number of seeds for PPR in comparison")

    # Seeds for PPR
    parser.add_argument("--seeds", type=str, help="Comma-separated seed node IDs")
    parser.add_argument("--seeds-file", type=str, help="File with seed IDs (one per line)")    
    parser.add_argument("--validate", action="store_true", help="Validate PageRank results")
    return parser.parse_args()


def load_or_generate(args: argparse.Namespace) -> List[Node]:
    if args.edge_list:
        return load_graph_from_edge_list(args.edge_list)
    nodes = generate_synthetic_graph(args.num_nodes, args.num_edges, seed=args.seed)
    if args.output_graph:
        nodes_to_json(args.output_graph, nodes)
    return nodes


def write_ranks(path: str, ranks: List[Tuple[int, float]]) -> None:
    with open(path, "w") as f:
        for nid, r in sorted(ranks, key=lambda x: x[0]):
            f.write(f"{nid}\t{r}\n")


def parse_seeds(args: argparse.Namespace) -> List[int]:
    if args.seeds_file:
        with open(args.seeds_file) as f:
            return [int(line.strip()) for line in f if line.strip()]
    if args.seeds:
        return [int(s) for s in args.seeds.split(",") if s.strip()]
    return []


def main():
    spark = SparkSession.builder.appName("PageRank CLI").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    args = parse_args()

    # Comparison mode: run multiple graphs, compute overlap, write JSON
    if args.compare:
        if not args.generate:
            print("--compare requires --generate (uses random graphs)")
            spark.stop()
            return

        results = {
            "runs": args.runs,
            "K": args.K,
            "d": args.d,
            "max_iter": args.max_iter,
            "epsilon": args.epsilon,
            "num_nodes": args.num_nodes,
            "num_edges": args.num_edges,
            "seeds_per_run": args.seeds_per_run,
            "per_run": [],
        }

        for i in range(args.runs):
            run_seed = args.seed + i
            rnd = random.Random(run_seed)
            nodes = generate_synthetic_graph(args.num_nodes, args.num_edges, seed=run_seed)
            rdd = sc.parallelize(nodes)

            # Optimized
            top_std, _, _ = rank_pages_optimized(
                rdd, K=args.K, maxIter=args.max_iter, epsilon=args.epsilon,
                d=args.d, track_timing=False, num_partitions=args.num_partitions
            )
            top_std_ids = [n.node_id for n, _ in top_std]

            # Personalized with random unique seeds
            seeds = rnd.sample(range(args.num_nodes), k=min(args.seeds_per_run, args.num_nodes))
            top_ppr, _, _ = rank_pages_personalized(
                rdd, seeds=seeds, K=args.K, maxIter=args.max_iter, epsilon=args.epsilon,
                d=args.d, track_timing=False, num_partitions=args.num_partitions
            )
            top_ppr_ids = [n.node_id for n, _ in top_ppr]

            overlap = len(set(top_std_ids) & set(top_ppr_ids))
            overlap_ratio = overlap / float(args.K) if args.K > 0 else 0.0

            results["per_run"].append({
                "run": i,
                "graph_seed": run_seed,
                "seeds": seeds,
                "topK_standard": top_std_ids,
                "topK_personalized": top_ppr_ids,
                "overlap_count": overlap,
                "overlap_ratio": overlap_ratio,
            })

        avg_overlap = sum(r["overlap_ratio"] for r in results["per_run"]) / max(len(results["per_run"]), 1)
        results["average_overlap_ratio"] = avg_overlap

        out_path = args.output_json or "comparison_results.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)
        print(f"Wrote comparison JSON to: {out_path}")

        spark.stop()
        return

    nodes = load_or_generate(args)

    rdd = sc.parallelize(nodes)
    if args.personalized:
        seeds = parse_seeds(args)
        top, all_ranks, stats = rank_pages_personalized(
            rdd, seeds, K=args.K, maxIter=args.max_iter, epsilon=args.epsilon,
            d=args.d, track_timing=True, num_partitions=args.num_partitions
        )
    elif args.optimized:
        top, all_ranks, stats = rank_pages_optimized(
            rdd, K=args.K, maxIter=args.max_iter, epsilon=args.epsilon,
            d=args.d, track_timing=True, num_partitions=args.num_partitions
        )
    elif args.optimized:
        # fallback to unoptimized if you didn’t keep a separate simple-optimized function
        top, all_ranks, stats = rank_pages_unoptimized(
            rdd, K=args.K, maxIter=args.max_iter, epsilon=args.epsilon,
            d=args.d, track_timing=True
        )
    else:
        top, all_ranks, stats = rank_pages_unoptimized(
            rdd, K=args.K, maxIter=args.max_iter, epsilon=args.epsilon,
            d=args.d, track_timing=True
        )

    for node, rank in top:
        print(f"node={node.node_id} rank={rank:.6f} out_degree={node.out_degree}")
    if stats.get("total_time") is not None:
        print(f"Total time: {stats['total_time']:.4f}s over {stats['iterations']} iterations")

    if args.output_ranks:
        write_ranks(args.output_ranks, all_ranks)
    
    # Validation if requested
    if args.validate:
        print("\n=== Validation ===")
        ranks_rdd = sc.parallelize(all_ranks)
        is_valid, details = validate(rdd, ranks_rdd, args.epsilon)
        if is_valid:
            print("Validation PASSED")
        else:
            print(f"Validation FAILED: {details.get('reason', 'Unknown')}")
        print(f"  Rank sum: {details.get('rank_sum', 'N/A'):.6f}")
        print(f"  Nodes: {details.get('num_nodes', 'N/A')}, Ranks: {details.get('num_ranks', 'N/A')}")
        print(f"  Dangling nodes: {details.get('dangling_nodes_count', 0)}")
        if details.get('dangling_nodes_count', 0) > 0:
            print(f"  Min dangling rank: {details.get('min_dangling_rank', 'N/A'):.6f}")

    spark.stop()


if __name__ == "__main__":
    main()
