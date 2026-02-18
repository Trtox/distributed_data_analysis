# OPOS Project - Big Data Analysis with Apache Spark

This repository contains two Apache Spark-based projects developed for distributed data processing:

1. **CitiBike Analysis** - Analysis of NYC CitiBike trip data with weather correlation
2. **PageRank** - Graph processing implementation with multiple PageRank algorithms

## Table of Contents

- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [CitiBike Analysis](#citibike-analysis)
- [PageRank](#pagerank)
- [Project Structure](#project-structure)

## Prerequisites

- Python 3.13+
- Apache Spark 4.x
- Java 17
- PySpark library

## Setup

### Install Dependencies

```bash
pip install pyspark
```

## CitiBike Analysis

A comprehensive analysis of NYC CitiBike trip data from October 2024, examining ride patterns, station usage, and weather impacts.

### Features

#### Core Analysis (`citibike.py`)

1. **Ride Pattern Analysis**
   - Rides by hour, day type (weekend/weekday), and ride type (short/medium/long)
   - Member vs. casual rider patterns
   - Duration distribution analysis

2. **Station Metrics**
   - Top stations by member ratio
   - Top stations by round trip ratio
   - Most active stations by total rides
   
3. **Route Analysis**
   - Most common routes with frequency counts
   - Median and average duration per route
   - Coefficient of variation for route consistency

4. **Round Trip Analysis**
   - Overall round trip vs. one-way distribution
   - Round trip patterns by membership type

#### Weather Impact Analysis (`weather_analysis.py`)

Analyzes how weather conditions affect bike usage:

- **Rain Impact**: Categorizes precipitation (clear, light, moderate, heavy, hazardous)
- **Temperature Impact**: Temperature bands (very cold, cold, mild, warm, hot)
- **Hourly Correlation**: Weather effects by hour of day
- **Membership Analysis**: Different weather sensitivities for members vs. casual riders

### Running CitiBike Analysis

```bash
cd CitiBike
spark-submit citibike.py
spark-submit weather_analysis.py
```

### Data Requirements

- **Trip Data**: Download October 2024 CitiBike trip data from the [official source](https://s3.amazonaws.com/tripdata/202410-citibike-tripdata.zip) and place CSV files in `data/tripdata/`
  - Expected files: `202410-citibike-tripdata_*.csv`

- **Weather Data**: Place weather CSV in `data/`
  - File: `KJRB0.csv` (hourly weather observations)

### Output

Results are saved in the following directories:

#### CitiBike_Analysis/
- `1_rides_by_hour_type/` - Ride counts by hour, member type, and ride duration
- `2a_stations_by_member_ratio/` - Stations ranked by member percentage
- `2b_stations_by_roundtrip_ratio/` - Stations ranked by round trip percentage
- `2c_stations_by_total_rides/` - Most active stations
- `3_most_common_routes/` - Popular routes with statistics
- `4a_roundtrip_overall/` - Overall round trip statistics
- `4b_roundtrip_by_member/` - Round trip comparison by membership

#### Weather_Analysis/
- `1_rain_impact_by_hour/` - Precipitation impact on ridership by hour
- `2_rain_impact_by_member/` - Rain sensitivity by membership type
- `3_temp_impact_comparison/` - Temperature effects on ride patterns

## PageRank

Implementation of PageRank algorithms for large-scale graph processing using Apache Spark.

### Features

- **Standard PageRank**: Classic algorithm implementation
- **Optimized PageRank**: Performance-optimized version with better partitioning
- **Personalized PageRank**: Topic-sensitive PageRank from seed nodes
- **Graph Generation**: Synthetic graph generation for testing
- **Validation**: Correctness verification tools

### Running PageRank

#### Basic Usage

```bash
cd PageRank

# Run with synthetic graph
python cli.py --generate --num-nodes 1000 --num-edges 4000 --K 20

# Run with edge list file
python cli.py --edge-list path/to/edges.txt --K 20

# Use optimized version
python cli.py --generate --num-nodes 10000 --optimized --num-partitions 16

# Personalized PageRank with seed nodes
python cli.py --generate --personalized --seeds 1,5,10,20

# Compare standard vs personalized
python cli.py --compare --runs 10 --seeds-per-run 5 --output-json results.json
```

#### Command-Line Options

**Graph Input:**
- `--edge-list <path>` - Load graph from edge list file
- `--generate` - Generate synthetic random graph
- `--num-nodes <n>` - Number of nodes for synthetic graph (default: 1000)
- `--num-edges <n>` - Number of edges for synthetic graph (default: 4000)
- `--seed <n>` - Random seed for generation (default: 42)

**Algorithm Parameters:**
- `--d <float>` - Damping factor (default: 0.85)
- `--max-iter <n>` - Maximum iterations (default: 20)
- `--epsilon <float>` - Convergence threshold (default: 0.001)
- `--K <n>` - Number of top-ranked nodes to return (default: 20)
- `--num-partitions <n>` - Number of partitions for optimized version

**Execution Modes:**
- `--optimized` - Use optimized PageRank implementation
- `--personalized` - Run Personalized PageRank
- `--compare` - Compare standard vs personalized over multiple runs
- `--validate` - Validate PageRank results

**Personalized PageRank Seeds:**
- `--seeds <ids>` - Comma-separated seed node IDs
- `--seeds-file <path>` - File with seed IDs (one per line)
- `--seeds-per-run <n>` - Seeds for PPR in comparison mode (default: 1)

**Output:**
- `--output-graph <path>` - Save generated graph to file
- `--output-ranks <path>` - Write final ranks as TSV
- `--output-json <path>` - Write comparison results to JSON

### Graph Format

Edge list format (space or tab separated):
```
source_id target_id
1 2
1 3
2 4
```

### Modules

- **`graph.py`**: Graph data structures and utilities
  - Node representation with in/out neighbors
  - Graph loading from edge lists
  - Synthetic graph generation
  
- **`pagerank.py`**: PageRank algorithm implementations
  - `rank_pages_unoptimized()` - Standard implementation
  - `rank_pages_optimized()` - Performance optimized
  - `rank_pages_personalized()` - Personalized PageRank
  - `validate()` - Result validation
  
- **`cli.py`**: Command-line interface and experiment runner

## Project Structure

```
.
├── CitiBike/
│   ├── citibike.py              # Main CitiBike analysis
│   ├── weather_analysis.py      # Weather correlation analysis
│   ├── CitiBike_Analysis/       # Analysis results
│   └── Weather_Analysis/        # Weather analysis results
│
├── PageRank/
│   ├── cli.py                   # Command-line interface
│   ├── graph.py                 # Graph structures
│   ├── pagerank.py              # PageRank algorithms
│   └── results.json             # Sample results
```

## Data Cleaning & Validation

### CitiBike Data

The analysis applies the following filters:
- Valid ride IDs and timestamps
- Positive ride duration (< 24 hours)
- Valid start and end stations
- Ended time after start time

Additional computed columns:
- `duration_minutes` - Ride length in minutes
- `hour_in_day` - Hour of day (0-23)
- `day_of_week` - Day name
- `weekend_indicator` - Weekend vs weekday flag
- `round_trip` - Same start/end station
- `ride_type` - Short (<15min), Medium (15-45min), Long (>45min)

### Weather Data

Weather data includes:
- Hourly temperature, humidity, precipitation
- Weather condition codes (coco)
- Wind speed and direction
- Atmospheric pressure
- Cloud cover

## Performance Considerations

### CitiBike Analysis
- Configured for 16GB driver memory
- Uses DataFrame caching for repeated operations
- Minimum ride thresholds to filter noise (default: 100 rides)

### PageRank
- Optimized version uses configurable partitioning
- Supports large graphs through RDD partitioning
- Includes timing and convergence tracking

## License

This project is released under the MIT license.

## Author

Created as a project for Selected topics in Operating Systems course.
