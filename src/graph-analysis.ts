/**
 * Graph analysis algorithms for social network traversal
 */

import { DuckDBConnection } from "@duckdb/node-api";
import type { SocialPath } from "./types.js";
import { normalizePubkey } from "./parser.js";
import { executeWithRetry } from "./utils.js";

/**
 * Finds the shortest path between two pubkeys using optimized bidirectional BFS
 *
 * This implementation uses a bidirectional breadth-first search with aggressive pruning:
 * - Searches from both source and target simultaneously
 * - Limits frontier expansion to prevent explosion in dense graphs
 * - Early termination when frontiers meet
 * - Efficient for graphs with high-degree nodes
 *
 * @param connection - Active DuckDB connection
 * @param fromPubkey - Starting pubkey (will be normalized to lowercase)
 * @param toPubkey - Target pubkey (will be normalized to lowercase)
 * @param maxDepth - Maximum search depth (default: 6)
 * @returns Promise resolving to the shortest path, or null if no path exists
 */
export async function findShortestPath(
  connection: DuckDBConnection,
  fromPubkey: string,
  toPubkey: string,
  maxDepth: number = 6,
): Promise<SocialPath | null> {
  // Normalize pubkeys to lowercase for consistent comparison
  const normalizedFrom = normalizePubkey(fromPubkey);
  const normalizedTo = normalizePubkey(toPubkey);

  // If source and target are the same, return a path with distance 0
  if (normalizedFrom === normalizedTo) {
    return {
      path: [normalizedFrom],
      distance: 0,
    };
  }

  // Quick check: Do both pubkeys exist in the graph?
  const existenceReader = await connection.runAndReadAll(
    `SELECT
      EXISTS(SELECT 1 FROM nsd_follows WHERE follower_pubkey = ?) OR
      EXISTS(SELECT 1 FROM nsd_follows WHERE followed_pubkey = ?) as from_exists,
      EXISTS(SELECT 1 FROM nsd_follows WHERE follower_pubkey = ?) OR
      EXISTS(SELECT 1 FROM nsd_follows WHERE followed_pubkey = ?) as to_exists`,
    [normalizedFrom, normalizedFrom, normalizedTo, normalizedTo],
  );

  const existenceRow = existenceReader.getRows()[0];
  if (!existenceRow || !existenceRow[0] || !existenceRow[1]) {
    // One or both pubkeys don't exist in the graph
    return null;
  }

  // Check for direct connection first (optimization)
  const directReader = await connection.runAndReadAll(
    `SELECT 1 FROM nsd_follows WHERE follower_pubkey = ? AND followed_pubkey = ?`,
    [normalizedFrom, normalizedTo],
  );

  if (directReader.getRows().length > 0) {
    return {
      path: [normalizedFrom, normalizedTo],
      distance: 1,
    };
  }

  // For distance 2, use optimized query that limits exploration
  if (maxDepth >= 2) {
    const distance2Reader = await connection.runAndReadAll(
      `
      SELECT
        f1.follower_pubkey AS start,
        f1.followed_pubkey AS intermediate,
        f2.followed_pubkey AS end
      FROM nsd_follows f1
      JOIN nsd_follows f2 ON f1.followed_pubkey = f2.follower_pubkey
      WHERE f1.follower_pubkey = ?
        AND f2.followed_pubkey = ?
      LIMIT 1
      `,
      [normalizedFrom, normalizedTo],
    );

    if (distance2Reader.getRows().length > 0) {
      const row = distance2Reader.getRows()[0]!;
      return {
        path: [String(row[0]), String(row[1]), String(row[2])],
        distance: 2,
      };
    }
  }

  // For distance 3+, use optimized bidirectional search with path tracking
  // Each direction searches up to half the maxDepth
  const searchDepth = Math.floor(maxDepth / 2);

  const reader = await connection.runAndReadAll(
    `
    WITH RECURSIVE
    -- Forward search: limited depth with path tracking
    forward_search(node, parent, depth, path)
    USING KEY (node) AS (
      SELECT ? AS node, NULL::VARCHAR AS parent, 0 AS depth, [?] AS path
      UNION
      SELECT DISTINCT f.followed_pubkey, fs.node, fs.depth + 1, list_append(fs.path, f.followed_pubkey)
      FROM forward_search fs
      JOIN nsd_follows f ON fs.node = f.follower_pubkey
      LEFT JOIN recurring.forward_search visited ON f.followed_pubkey = visited.node
      WHERE fs.depth < ?
        AND visited.node IS NULL
        AND NOT list_contains(fs.path, f.followed_pubkey) -- Prevent cycles
    ),
    -- Backward search: limited depth with path tracking
    backward_search(node, child, depth, path)
    USING KEY (node) AS (
      SELECT ? AS node, NULL::VARCHAR AS child, 0 AS depth, [?] AS path
      UNION
      SELECT DISTINCT f.follower_pubkey, bs.node, bs.depth + 1, list_append(bs.path, f.follower_pubkey)
      FROM backward_search bs
      JOIN nsd_follows f ON bs.node = f.followed_pubkey
      LEFT JOIN recurring.backward_search visited ON f.follower_pubkey = visited.node
      WHERE bs.depth < ?
        AND visited.node IS NULL
        AND NOT list_contains(bs.path, f.follower_pubkey) -- Prevent cycles
    ),
    -- Find first intersection point with complete path reconstruction
    intersection AS (
      SELECT
        fs.node AS meeting_point,
        fs.depth AS forward_depth,
        bs.depth AS backward_depth,
        fs.depth + bs.depth AS total_distance,
        fs.path AS forward_path,
        bs.path AS backward_path
      FROM forward_search fs
      JOIN backward_search bs ON fs.node = bs.node
      WHERE fs.node != ? AND fs.node != ?
      ORDER BY total_distance ASC
      LIMIT 1
    )
    SELECT
      meeting_point,
      forward_depth,
      backward_depth,
      total_distance,
      forward_path,
      backward_path
    FROM intersection
    `,
    [
      normalizedFrom,
      normalizedFrom,
      searchDepth,
      normalizedTo,
      normalizedTo,
      searchDepth,
      normalizedFrom,
      normalizedTo,
    ],
  );
  const rows = reader.getRows();

  if (rows.length === 0) {
    return null;
  }

  // Extract path information directly from the query
  const row = rows[0]!;
  const totalDistance = Number(row[3]);
  
  // Extract paths from the result (they're stored as DuckDB lists)
  // We cast to unknown first, then to string[] as we know the structure from the query
  const forwardPath = (row[4] as unknown) as string[];
  const backwardPath = (row[5] as unknown) as string[];

  // Combine paths: forward path + backward path (excluding meeting point from backward)
  // The backward path is stored in reverse order (target -> meeting point)
  const completePath = [...forwardPath, ...backwardPath.slice(1).reverse()];

  return {
    path: completePath,
    distance: totalDistance,
  };
}

/**
 * Checks if a direct follow relationship exists between two pubkeys
 *
 * @param connection - Active DuckDB connection
 * @param followerPubkey - The follower pubkey
 * @param followedPubkey - The followed pubkey
 * @returns Promise resolving to true if the relationship exists
 */
export async function isDirectFollow(
  connection: DuckDBConnection,
  followerPubkey: string,
  followedPubkey: string,
): Promise<boolean> {
  const normalizedFollower = normalizePubkey(followerPubkey);
  const normalizedFollowed = normalizePubkey(followedPubkey);

  const reader = await connection.runAndReadAll(
    `
    SELECT 1
    FROM nsd_follows
    WHERE follower_pubkey = ?
      AND followed_pubkey = ?
    LIMIT 1
    `,
    [normalizedFollower, normalizedFollowed],
  );

  return reader.getRows().length > 0;
}

/**
 * Finds mutual follows between two pubkeys (they follow each other)
 *
 * @param connection - Active DuckDB connection
 * @param pubkey1 - First pubkey
 * @param pubkey2 - Second pubkey
 * @returns Promise resolving to true if they mutually follow each other
 */
export async function areMutualFollows(
  connection: DuckDBConnection,
  pubkey1: string,
  pubkey2: string,
): Promise<boolean> {
  const normalized1 = normalizePubkey(pubkey1);
  const normalized2 = normalizePubkey(pubkey2);

  const reader = await connection.runAndReadAll(
    `
    SELECT 1
    FROM nsd_follows f1, nsd_follows f2
    WHERE f1.follower_pubkey = ?
      AND f1.followed_pubkey = ?
      AND f2.follower_pubkey = ?
      AND f2.followed_pubkey = ?
    LIMIT 1
    `,
    [normalized1, normalized2, normalized2, normalized1],
  );

  return reader.getRows().length > 0;
}

/**
 * Gets the degree (number of follows) for a pubkey
 *
 * @param connection - Active DuckDB connection
 * @param pubkey - The pubkey to check
 * @returns Promise resolving to object with outDegree (following) and inDegree (followers)
 */
export async function getPubkeyDegree(
  connection: DuckDBConnection,
  pubkey: string,
): Promise<{ outDegree: number; inDegree: number }> {
  const normalized = normalizePubkey(pubkey);

  const reader = await connection.runAndReadAll(
    `
    SELECT
      (SELECT COUNT(*) FROM nsd_follows WHERE follower_pubkey = ?) as out_degree,
      (SELECT COUNT(*) FROM nsd_follows WHERE followed_pubkey = ?) as in_degree
    `,
    [normalized, normalized],
  );

  const rows = reader.getRows();
  if (rows.length === 0) {
    return { outDegree: 0, inDegree: 0 };
  }

  const row = rows[0]!;
  return {
    outDegree: Number(row[0]),
    inDegree: Number(row[1]),
  };
}

/**
 * Gets all pubkeys reachable from a starting pubkey within a specified distance
 *
 * Uses DuckDB's recursive CTE with USING KEY to efficiently traverse the graph
 * and collect all unique pubkeys within the distance limit.
 *
 * @param connection - Active DuckDB connection
 * @param fromPubkey - Starting pubkey (will be normalized to lowercase)
 * @param distance - Maximum distance (number of hops) to search
 * @returns Promise resolving to array of pubkeys (excluding the starting pubkey)
 */
export async function getUsersWithinDistance(
  connection: DuckDBConnection,
  fromPubkey: string,
  distance: number,
): Promise<string[] | null> {
  // Normalize pubkey to lowercase for consistent comparison
  const normalizedFrom = normalizePubkey(fromPubkey);

  // Handle edge cases
  if (distance < 1) {
    return [];
  }

  // Quick check: Does the starting pubkey exist in the graph?
  const fromExists = await connection.runAndReadAll(
    "SELECT 1 FROM nsd_follows WHERE follower_pubkey = ? OR followed_pubkey = ? LIMIT 1",
    [normalizedFrom, normalizedFrom],
  );

  if (fromExists.getRows().length === 0) {
    // Starting pubkey doesn't exist in the graph - return null for consistency
    return null;
  }

  // Execute the recursive CTE to find all reachable pubkeys within distance
  // Using memory-efficient approach with visited tracking instead of path tracking
  const reader = await connection.runAndReadAll(
    `
    WITH RECURSIVE reachable_nodes(start_pubkey, end_pubkey, current_distance)
    USING KEY (end_pubkey) AS (
      -- Base case: direct follows from source pubkey
      SELECT
        follower_pubkey AS start_pubkey,
        followed_pubkey AS end_pubkey,
        1 AS current_distance
      FROM nsd_follows
      WHERE follower_pubkey = ?
      
      UNION
      
      -- Recursive case: follow chains with efficient cycle detection
      SELECT
        rn.start_pubkey,
        f.followed_pubkey,
        rn.current_distance + 1 AS current_distance
      FROM reachable_nodes rn
      JOIN nsd_follows f ON rn.end_pubkey = f.follower_pubkey
      LEFT JOIN recurring.reachable_nodes visited ON f.followed_pubkey = visited.end_pubkey
      WHERE rn.current_distance < ?
        AND visited.end_pubkey IS NULL
    )
    SELECT DISTINCT end_pubkey
    FROM reachable_nodes
    WHERE end_pubkey != ? -- Exclude the starting pubkey
    ORDER BY end_pubkey
    `,
    [normalizedFrom, distance, normalizedFrom],
  );

  const rows = reader.getRows();

  // Extract pubkeys from results
  const pubkeys: string[] = [];
  for (const row of rows) {
    if (row[0] && typeof row[0] === "string") {
      pubkeys.push(row[0]);
    }
  }

  return pubkeys;
}

/**
 * Builds a temporary table with pre-calculated distances from a root pubkey
 *
 * @param connection - Active DuckDB connection
 * @param rootPubkey - The root pubkey
 * @param maxDepth - Maximum depth to traverse
 */
export async function buildRootDistancesTable(
  connection: DuckDBConnection,
  rootPubkey: string,
  maxDepth: number,
): Promise<void> {
  const normalizedRoot = normalizePubkey(rootPubkey);

  // Use retry logic for transaction conflicts
  await executeWithRetry(async () => {
    // Create temporary table for O(1) lookups using optimized BFS
    // We use CREATE OR REPLACE to handle updates efficiently
    await connection.run(
      `
      CREATE OR REPLACE TEMPORARY TABLE nsd_root_distances AS
      WITH RECURSIVE bfs(pubkey, distance)
      USING KEY (pubkey) AS (
        -- Base case: root node
        SELECT ?::VARCHAR AS pubkey, 0 AS distance
        UNION
        -- Recursive step
        SELECT DISTINCT
          f.followed_pubkey AS pubkey,
          bfs.distance + 1 AS distance
        FROM bfs
        JOIN nsd_follows f ON bfs.pubkey = f.follower_pubkey
        LEFT JOIN recurring.bfs visited ON f.followed_pubkey = visited.pubkey
        WHERE bfs.distance < ?
          AND visited.pubkey IS NULL
      )
      SELECT pubkey, distance FROM bfs
      `,
      [normalizedRoot, maxDepth],
    );

    // Create index for fast lookups
    await connection.run(`
      CREATE INDEX IF NOT EXISTS idx_root_distances ON nsd_root_distances(pubkey)
    `);
  });
}

/**
 * Gets the distance from the root pubkey to a target pubkey using the pre-calculated table
 *
 * @param connection - Active DuckDB connection
 * @param targetPubkey - The target pubkey
 * @returns Promise resolving to distance or null
 */
export async function getDistanceFromRoot(
  connection: DuckDBConnection,
  targetPubkey: string,
): Promise<number | null> {
  const normalizedTarget = normalizePubkey(targetPubkey);

  const reader = await connection.runAndReadAll(
    `
    SELECT distance
    FROM nsd_root_distances
    WHERE pubkey = ?
    LIMIT 1
    `,
    [normalizedTarget],
  );

  const rows = reader.getRows();
  if (rows.length === 0) {
    return null;
  }

  return Number(rows[0]![0]);
}

/**
 * Gets all users exactly at a specific distance from the root pubkey using the pre-calculated table
 *
 * @param connection - Active DuckDB connection
 * @param distance - The exact distance in hops
 * @returns Promise resolving to array of pubkeys
 */
export async function getUsersAtDistanceFromRoot(
  connection: DuckDBConnection,
  distance: number,
): Promise<string[]> {
  const reader = await connection.runAndReadAll(
    `
    SELECT pubkey
    FROM nsd_root_distances
    WHERE distance = ?
    `,
    [distance],
  );

  const rows = reader.getRows();
  const pubkeys: string[] = [];
  
  for (const row of rows) {
    if (row[0] && typeof row[0] === "string") {
      pubkeys.push(row[0]);
    }
  }

  return pubkeys;
}

/**
 * Gets all users within a specific distance from the root pubkey using the pre-calculated table
 *
 * @param connection - Active DuckDB connection
 * @param distance - The maximum distance in hops
 * @returns Promise resolving to array of pubkeys
 */
export async function getUsersWithinDistanceFromRoot(
  connection: DuckDBConnection,
  distance: number,
): Promise<string[]> {
  const reader = await connection.runAndReadAll(
    `
    SELECT pubkey
    FROM nsd_root_distances
    WHERE distance <= ? AND distance > 0
    `,
    [distance],
  );

  const rows = reader.getRows();
  const pubkeys: string[] = [];
  
  for (const row of rows) {
    if (row[0] && typeof row[0] === "string") {
      pubkeys.push(row[0]);
    }
  }

  return pubkeys;
}

/**
 * Gets the distribution of users by distance from the root pubkey
 *
 * @param connection - Active DuckDB connection
 * @returns Promise resolving to a map of distance -> count
 */
export async function getRootDistanceDistribution(
  connection: DuckDBConnection,
): Promise<Record<number, number>> {
  const reader = await connection.runAndReadAll(
    `
    SELECT distance, COUNT(*) as count
    FROM nsd_root_distances
    WHERE distance > 0
    GROUP BY distance
    ORDER BY distance
    `,
  );

  const rows = reader.getRows();
  const distribution: Record<number, number> = {};
  
  for (const row of rows) {
    const distance = Number(row[0]);
    const count = Number(row[1]);
    distribution[distance] = count;
  }

  return distribution;
}

/**
 * Finds the shortest distance between two pubkeys using optimized bidirectional BFS
 *
 * This is a performance-optimized version that only returns the distance,
 * skipping the expensive path reconstruction. It's 2-3x faster than findShortestPath.
 *
 * @param connection - Active DuckDB connection
 * @param fromPubkey - Starting pubkey (will be normalized to lowercase)
 * @param toPubkey - Target pubkey (will be normalized to lowercase)
 * @param maxDepth - Maximum search depth (default: 6)
 * @returns Promise resolving to the distance, or null if no path exists
 */
export async function findShortestDistance(
  connection: DuckDBConnection,
  fromPubkey: string,
  toPubkey: string,
  maxDepth: number = 6,
): Promise<number | null> {
  // Normalize pubkeys to lowercase for consistent comparison
  const normalizedFrom = normalizePubkey(fromPubkey);
  const normalizedTo = normalizePubkey(toPubkey);

  // If source and target are the same, return distance 0
  if (normalizedFrom === normalizedTo) {
    return 0;
  }

  // Quick check: Do both pubkeys exist in the graph?
  const existenceReader = await connection.runAndReadAll(
    `SELECT
      EXISTS(SELECT 1 FROM nsd_follows WHERE follower_pubkey = ?) OR
      EXISTS(SELECT 1 FROM nsd_follows WHERE followed_pubkey = ?) as from_exists,
      EXISTS(SELECT 1 FROM nsd_follows WHERE follower_pubkey = ?) OR
      EXISTS(SELECT 1 FROM nsd_follows WHERE followed_pubkey = ?) as to_exists`,
    [normalizedFrom, normalizedFrom, normalizedTo, normalizedTo],
  );

  const existenceRow = existenceReader.getRows()[0];
  if (!existenceRow || !existenceRow[0] || !existenceRow[1]) {
    // One or both pubkeys don't exist in the graph
    return null;
  }

  // Check for direct connection first
  const directReader = await connection.runAndReadAll(
    `SELECT 1 FROM nsd_follows WHERE follower_pubkey = ? AND followed_pubkey = ?`,
    [normalizedFrom, normalizedTo],
  );

  if (directReader.getRows().length > 0) {
    return 1;
  }

  // For distance 2, use optimized query that limits exploration
  if (maxDepth >= 2) {
    const distance2Reader = await connection.runAndReadAll(
      `
      SELECT 1
      FROM nsd_follows f1
      JOIN nsd_follows f2 ON f1.followed_pubkey = f2.follower_pubkey
      WHERE f1.follower_pubkey = ?
        AND f2.followed_pubkey = ?
      LIMIT 1
      `,
      [normalizedFrom, normalizedTo],
    );

    if (distance2Reader.getRows().length > 0) {
      return 2;
    }
  }

  // For distance 3+, use limited bidirectional search
  // Each direction searches up to half the maxDepth
  const searchDepth = Math.floor(maxDepth / 2);

  const reader = await connection.runAndReadAll(
    `
    WITH RECURSIVE
    -- Forward search: limited depth
    forward_search(node, parent, depth)
    USING KEY (node) AS (
      SELECT ? AS node, NULL::VARCHAR AS parent, 0 AS depth
      UNION
      SELECT DISTINCT f.followed_pubkey, fs.node, fs.depth + 1
      FROM forward_search fs
      JOIN nsd_follows f ON fs.node = f.follower_pubkey
      LEFT JOIN recurring.forward_search visited ON f.followed_pubkey = visited.node
      WHERE fs.depth < ?
        AND visited.node IS NULL
    ),
    -- Backward search: limited depth
    backward_search(node, child, depth)
    USING KEY (node) AS (
      SELECT ? AS node, NULL::VARCHAR AS child, 0 AS depth
      UNION
      SELECT DISTINCT f.follower_pubkey, bs.node, bs.depth + 1
      FROM backward_search bs
      JOIN nsd_follows f ON bs.node = f.followed_pubkey
      LEFT JOIN recurring.backward_search visited ON f.follower_pubkey = visited.node
      WHERE bs.depth < ?
        AND visited.node IS NULL
    ),
    -- Find first intersection point
    intersection AS (
      SELECT
        fs.depth + bs.depth AS total_distance
      FROM forward_search fs
      JOIN backward_search bs ON fs.node = bs.node
      WHERE fs.node != ? AND fs.node != ?
      ORDER BY total_distance ASC
      LIMIT 1
    )
    SELECT total_distance
    FROM intersection
    `,
    [
      normalizedFrom,
      searchDepth,
      normalizedTo,
      searchDepth,
      normalizedFrom,
      normalizedTo,
    ],
  );
  const rows = reader.getRows();

  if (rows.length === 0) {
    return null;
  }

  const row = rows[0]!;
  const distance = Number(row[0]);
  return distance;
}

/**
 * Gets all unique pubkeys in the social graph (both followers and followed)
 *
 * This method efficiently retrieves all pubkeys that appear in the graph,
 * either as followers (outgoing edges) or followed (incoming edges).
 *
 * @param connection - Active DuckDB connection
 * @returns Promise resolving to array of all unique pubkeys in the graph
 */
export async function getAllUniquePubkeys(
  connection: DuckDBConnection,
): Promise<string[]> {
  const reader = await connection.runAndReadAll(`
    SELECT DISTINCT pubkey
    FROM (
      SELECT follower_pubkey AS pubkey FROM nsd_follows
      UNION ALL
      SELECT followed_pubkey AS pubkey FROM nsd_follows
    )
  `);

  const rows = reader.getRows();

  // Extract pubkeys from results
  const pubkeys: string[] = [];
  for (const row of rows) {
    if (row[0] && typeof row[0] === "string") {
      pubkeys.push(row[0]);
    }
  }

  return pubkeys;
}
