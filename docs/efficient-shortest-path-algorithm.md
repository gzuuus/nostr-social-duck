# Efficient Shortest Path Algorithm for Nostr Social Duck

## Executive Summary

This document outlines a high-performance algorithm design for finding shortest paths in dense Nostr social graphs. The current implementation using recursive CTEs with `USING KEY` times out on graphs with 779 followers and 39,949 followed pubkeys when searching for distance-2 paths. We propose a **hybrid bidirectional BFS approach with intelligent pruning** that can handle this workload efficiently.

## Problem Analysis

### Current State

**Graph Characteristics:**

- 376,889 total follow relationships
- 779 unique followers (sources)
- 39,949 unique followed pubkeys (destinations)
- **Highly skewed degree distribution** (some nodes have 973 outgoing edges)
- Average out-degree: ~484 follows per follower
- This is a **dense, hub-dominated graph**

**Current Implementation Issues:**

1. **Path explosion**: With 973 outgoing edges from a single node, exploring all paths creates exponential growth
2. **No early termination**: DuckDB processes all paths up to maxDepth before filtering
3. **Memory pressure**: Even with `USING KEY`, the working table grows too large
4. **Cycle detection overhead**: `list_contains()` on growing path arrays is expensive

### Why Current Approach Fails

```
Distance 1: ~484 paths (direct follows)
Distance 2: ~484 × 484 = ~234,256 potential paths
```

Even with `USING KEY` deduplication, the query must:

1. Generate all distance-1 paths
2. For each, join with follows table (973 edges per high-degree node)
3. Check cycles with `list_contains()` on each path
4. Filter to target only at the end

**Result**: Timeout before completion.

## Proposed Solution: Hybrid Bidirectional BFS

### Core Algorithm Design

We'll implement a **bidirectional breadth-first search (BFS)** with **intelligent pruning** that leverages DuckDB's strengths while avoiding its weaknesses.

### Key Innovations

1. **Bidirectional Search**: Search from both source and target simultaneously
2. **Degree-Based Pruning**: Prioritize low-degree nodes to reduce branching
3. **Early Intersection Detection**: Stop as soon as frontiers meet
4. **Materialized Frontiers**: Use temporary tables instead of recursive CTEs
5. **Batch Processing**: Process frontiers in controlled batches

### Algorithm Pseudocode

```
function findShortestPath(source, target, maxDepth):
    if source == target:
        return [source], 0

    # Check existence
    if not exists(source) or not exists(target):
        return null

    # Get node degrees for pruning decisions
    sourceDegree = getOutDegree(source)
    targetDegree = getInDegree(target)

    # Choose search direction based on degrees
    if sourceDegree <= targetDegree:
        return bidirectionalBFS(source, target, maxDepth, FORWARD_BIAS)
    else:
        return bidirectionalBFS(target, source, maxDepth, BACKWARD_BIAS)

function bidirectionalBFS(source, target, maxDepth, direction):
    # Initialize frontiers
    forwardFrontier = {source}
    backwardFrontier = {target}
    forwardVisited = {source: null}  # node -> parent
    backwardVisited = {target: null}

    forwardDepth = 0
    backwardDepth = 0

    while forwardDepth + backwardDepth < maxDepth:
        # Choose which frontier to expand (smaller one)
        if size(forwardFrontier) <= size(backwardFrontier):
            # Expand forward
            forwardDepth++
            newFrontier = expandForward(forwardFrontier, forwardVisited)

            # Check for intersection
            intersection = newFrontier ∩ backwardVisited
            if intersection not empty:
                return reconstructPath(intersection, forwardVisited, backwardVisited)

            forwardFrontier = newFrontier
        else:
            # Expand backward
            backwardDepth++
            newFrontier = expandBackward(backwardFrontier, backwardVisited)

            # Check for intersection
            intersection = newFrontier ∩ forwardVisited
            if intersection not empty:
                return reconstructPath(intersection, forwardVisited, backwardVisited)

            backwardFrontier = newFrontier

    return null  # No path within maxDepth

function expandForward(frontier, visited):
    newFrontier = {}

    # Get all neighbors of frontier nodes
    neighbors = SELECT DISTINCT followed_pubkey, follower_pubkey
                FROM nsd_follows
                WHERE follower_pubkey IN frontier
                  AND followed_pubkey NOT IN visited

    for (neighbor, parent) in neighbors:
        if neighbor not in visited:
            visited[neighbor] = parent
            newFrontier.add(neighbor)

    return newFrontier

function expandBackward(frontier, visited):
    newFrontier = {}

    # Get all predecessors of frontier nodes
    predecessors = SELECT DISTINCT follower_pubkey, followed_pubkey
                   FROM nsd_follows
                   WHERE followed_pubkey IN frontier
                     AND follower_pubkey NOT IN visited

    for (predecessor, child) in predecessors:
        if predecessor not in visited:
            visited[predecessor] = child
            newFrontier.add(predecessor)

    return newFrontier
```

## DuckDB Implementation Strategy

### Phase 1: Temporary Table-Based BFS

Instead of recursive CTEs, use temporary tables to materialize frontiers:

```sql
-- Initialize forward frontier
CREATE TEMP TABLE forward_frontier AS
SELECT follower_pubkey AS node, 0 AS depth
FROM (VALUES (?)) AS t(follower_pubkey);

CREATE TEMP TABLE forward_visited AS
SELECT node, NULL::VARCHAR AS parent FROM forward_frontier;

-- Initialize backward frontier
CREATE TEMP TABLE backward_frontier AS
SELECT followed_pubkey AS node, 0 AS depth
FROM (VALUES (?)) AS t(followed_pubkey);

CREATE TEMP TABLE backward_visited AS
SELECT node, NULL::VARCHAR AS parent FROM backward_frontier;

-- Iterative expansion (controlled from application)
WHILE forward_depth + backward_depth < maxDepth:
    -- Choose smaller frontier
    IF (SELECT COUNT(*) FROM forward_frontier) <=
       (SELECT COUNT(*) FROM backward_frontier):

        -- Expand forward
        CREATE TEMP TABLE new_forward AS
        SELECT DISTINCT f.followed_pubkey AS node,
                        ff.node AS parent
        FROM forward_frontier ff
        JOIN nsd_follows f ON ff.node = f.follower_pubkey
        WHERE f.followed_pubkey NOT IN (SELECT node FROM forward_visited);

        -- Check intersection
        SELECT nf.node, nf.parent AS forward_parent,
               bv.parent AS backward_parent
        FROM new_forward nf
        JOIN backward_visited bv ON nf.node = bv.node;

        -- If intersection found, reconstruct path and return
        -- Otherwise, update frontier and visited

    ELSE:
        -- Expand backward (similar logic)
```

### Phase 2: Optimized Single-Query Approach

For better performance, implement a controlled recursive CTE with early termination:

```sql
WITH RECURSIVE
-- Forward search
forward_search(node, parent, depth) AS (
    SELECT ? AS node, NULL::VARCHAR AS parent, 0 AS depth
    UNION ALL
    SELECT f.followed_pubkey, fs.node, fs.depth + 1
    FROM forward_search fs
    JOIN nsd_follows f ON fs.node = f.follower_pubkey
    WHERE fs.depth < ?
      AND NOT EXISTS (
          SELECT 1 FROM forward_search fs2
          WHERE fs2.node = f.followed_pubkey
      )
),
-- Backward search
backward_search(node, child, depth) AS (
    SELECT ? AS node, NULL::VARCHAR AS child, 0 AS depth
    UNION ALL
    SELECT f.follower_pubkey, bs.node, bs.depth + 1
    FROM backward_search bs
    JOIN nsd_follows f ON bs.node = f.followed_pubkey
    WHERE bs.depth < ?
      AND NOT EXISTS (
          SELECT 1 FROM backward_search bs2
          WHERE bs2.node = f.follower_pubkey
      )
),
-- Find intersection
intersection AS (
    SELECT fs.node AS meeting_point,
           fs.depth AS forward_depth,
           bs.depth AS backward_depth,
           fs.depth + bs.depth AS total_distance
    FROM forward_search fs
    JOIN backward_search bs ON fs.node = bs.node
    WHERE fs.node != ? AND fs.node != ?  -- Exclude source and target
    ORDER BY total_distance ASC
    LIMIT 1
)
SELECT * FROM intersection;
```

### Phase 3: Degree-Aware Pruning

Add intelligent pruning based on node degrees:

```sql
-- Pre-compute node degrees
CREATE TEMP TABLE node_degrees AS
SELECT follower_pubkey AS node,
       COUNT(*) AS out_degree
FROM nsd_follows
GROUP BY follower_pubkey;

-- Modified expansion with degree-based pruning
WITH RECURSIVE forward_search(node, parent, depth, cumulative_branching) AS (
    SELECT ? AS node, NULL::VARCHAR AS parent, 0 AS depth, 1 AS cumulative_branching
    UNION ALL
    SELECT f.followed_pubkey, fs.node, fs.depth + 1,
           fs.cumulative_branching * COALESCE(nd.out_degree, 1)
    FROM forward_search fs
    JOIN nsd_follows f ON fs.node = f.follower_pubkey
    LEFT JOIN node_degrees nd ON f.followed_pubkey = nd.node
    WHERE fs.depth < ?
      AND fs.cumulative_branching < 100000  -- Prune explosive branches
      AND NOT EXISTS (
          SELECT 1 FROM forward_search fs2
          WHERE fs2.node = f.followed_pubkey
      )
)
SELECT * FROM forward_search;
```

## Performance Optimizations

### 1. Index Strategy

Ensure optimal indexes exist:

```sql
-- Existing indexes (already in place)
CREATE INDEX idx_nsd_follows_follower ON nsd_follows(follower_pubkey);
CREATE INDEX idx_nsd_follows_followed ON nsd_follows(followed_pubkey);

-- Additional covering index for bidirectional search
CREATE INDEX idx_nsd_follows_both ON nsd_follows(follower_pubkey, followed_pubkey);
```

### 2. Frontier Size Limits

Implement hard limits on frontier sizes:

```typescript
const MAX_FRONTIER_SIZE = 10000;

if (frontierSize > MAX_FRONTIER_SIZE) {
  // Switch to sampling or heuristic search
  // Or return "path too complex" error
}
```

### 3. Caching Strategy

For repeated queries, cache intermediate results:

```typescript
interface CachedFrontier {
  source: string;
  depth: number;
  nodes: Set<string>;
  timestamp: number;
}

const frontierCache = new Map<string, CachedFrontier>();
```

### 4. Parallel Expansion

For very large frontiers, split into batches:

```sql
-- Process frontier in batches
WITH frontier_batches AS (
    SELECT node,
           ROW_NUMBER() OVER (ORDER BY node) % 10 AS batch_id
    FROM forward_frontier
)
SELECT f.followed_pubkey, fb.node AS parent
FROM frontier_batches fb
JOIN nsd_follows f ON fb.node = f.follower_pubkey
WHERE fb.batch_id = ?  -- Process one batch at a time
  AND f.followed_pubkey NOT IN (SELECT node FROM forward_visited);
```

## Algorithm Complexity Analysis

### Time Complexity

**Bidirectional BFS:**

- Best case: O(b^(d/2)) where b = branching factor, d = distance
- Worst case: O(b^d) (same as unidirectional, but with smaller constant)

**For your graph:**

- Average branching: ~484
- Distance 2 search:
  - Unidirectional: 484^2 = ~234,256 nodes explored
  - Bidirectional: 2 × 484^1 = ~968 nodes explored
  - **Speedup: ~242x**

### Space Complexity

**Memory usage:**

- Forward visited: O(b^(d/2))
- Backward visited: O(b^(d/2))
- Total: O(2 × b^(d/2)) vs O(b^d) for unidirectional

**For distance 2:**

- Bidirectional: ~2 × 484 = ~968 nodes in memory
- Unidirectional: ~234,256 nodes in memory
- **Memory reduction: ~242x**
