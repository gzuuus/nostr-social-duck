/**
 * Graph analysis algorithms for social network traversal
 */

import { DuckDBConnection } from "@duckdb/node-api";
import type { SocialPath } from "./types.js";
import { normalizePubkey } from "./parser.js";
import { executeWithRetry } from "./utils.js";
import { pubkeyExists } from "./database.js";

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

/**
 * Checks if both pubkeys exist in the graph
 */
async function checkGraphExistence(
  connection: DuckDBConnection,
  pubkey1: string,
  pubkey2: string,
): Promise<boolean> {
  const reader = await connection.runAndReadAll(
    `SELECT
      EXISTS(SELECT 1 FROM nsd_follows WHERE follower_pubkey = ?) OR
      EXISTS(SELECT 1 FROM nsd_follows WHERE followed_pubkey = ?) as exists1,
      EXISTS(SELECT 1 FROM nsd_follows WHERE follower_pubkey = ?) OR
      EXISTS(SELECT 1 FROM nsd_follows WHERE followed_pubkey = ?) as exists2`,
    [pubkey1, pubkey1, pubkey2, pubkey2],
  );

  const row = reader.getRows()[0];
  return !!(row && row[0] && row[1]);
}

/**
 * Checks for direct follow relationship
 */
async function checkDirectConnection(
  connection: DuckDBConnection,
  fromPubkey: string,
  toPubkey: string,
): Promise<boolean> {
  const reader = await connection.runAndReadAll(
    `SELECT 1 FROM nsd_follows WHERE follower_pubkey = ? AND followed_pubkey = ?`,
    [fromPubkey, toPubkey],
  );
  return reader.getRows().length > 0;
}

/**
 * Checks for distance 2 connection
 */
async function checkDistance2Connection(
  connection: DuckDBConnection,
  fromPubkey: string,
  toPubkey: string,
): Promise<{ path: string[]; distance: number } | null> {
  const reader = await connection.runAndReadAll(
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
    [fromPubkey, toPubkey],
  );

  if (reader.getRows().length > 0) {
    const row = reader.getRows()[0]!;
    return {
      path: [String(row[0]), String(row[1]), String(row[2])],
      distance: 2,
    };
  }
  return null;
}

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
  if (!(await checkGraphExistence(connection, normalizedFrom, normalizedTo))) {
    return null;
  }

  // Check for direct connection first (optimization)
  if (await checkDirectConnection(connection, normalizedFrom, normalizedTo)) {
    return {
      path: [normalizedFrom, normalizedTo],
      distance: 1,
    };
  }

  // For distance 2, use optimized query that limits exploration
  if (maxDepth >= 2) {
    const distance2Result = await checkDistance2Connection(connection, normalizedFrom, normalizedTo);
    if (distance2Result) {
      return distance2Result;
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

  return checkDirectConnection(connection, normalizedFollower, normalizedFollowed);
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
  if (!(await pubkeyExists(connection, normalizedFrom))) {
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
/**
 * Sets a value in the metadata table
 */
export async function setMetadataValue(
  connection: DuckDBConnection,
  key: string,
  value: string,
): Promise<void> {
  await executeWithRetry(async () => {
    await connection.run(
      `INSERT OR REPLACE INTO nsd_metadata (key, value) VALUES (?, ?)`,
      [key, value],
    );
  });
}

/**
 * Gets a value from the metadata table
 */
export async function getMetadataValue(
  connection: DuckDBConnection,
  key: string,
): Promise<string | null> {
  const reader = await connection.runAndReadAll(
    `SELECT value FROM nsd_metadata WHERE key = ?`,
    [key],
  );
  const rows = reader.getRows();
  if (rows.length === 0) {
    return null;
  }
  return String(rows[0]![0]);
}

export async function buildRootDistancesTable(
  connection: DuckDBConnection,
  rootPubkey: string,
  maxDepth: number,
): Promise<void> {
  const normalizedRoot = normalizePubkey(rootPubkey);
  const BATCH_SIZE = 5000;

  // Use retry logic for transaction conflicts
  await executeWithRetry(async () => {
    // Start transaction for atomic operation
    await connection.run("BEGIN TRANSACTION");
    try {
      // Create persistent table for O(1) lookups
      // We define PRIMARY KEY immediately to ensure fast lookups during the build process
      await connection.run(
        `
        CREATE OR REPLACE TABLE nsd_root_distances (
          pubkey VARCHAR(64) PRIMARY KEY,
          distance INTEGER NOT NULL
        );
        `
      );

      // Create temporary tables for BFS traversal
      // These are created once and reused to avoid catalog overhead
      await connection.run(
        `CREATE OR REPLACE TEMPORARY TABLE nsd_bfs_frontier (pubkey VARCHAR(64))`
      );
      await connection.run(
        `CREATE OR REPLACE TEMPORARY TABLE nsd_bfs_next_frontier (pubkey VARCHAR(64))`
      );
      await connection.run(
        `CREATE OR REPLACE TEMPORARY TABLE nsd_batch_candidates (pubkey VARCHAR(64))`
      );

      // Initialize: Insert root into both tables
      await connection.run(
        `INSERT INTO nsd_root_distances (pubkey, distance) VALUES (?, 0)`,
        [normalizedRoot]
      );
      await connection.run(
        `INSERT INTO nsd_bfs_frontier (pubkey) VALUES (?)`,
        [normalizedRoot]
      );

      // Iterative layer-by-layer construction
      for (let currentDepth = 0; currentDepth < maxDepth; currentDepth++) {
        // Check if frontier is empty (stop condition)
        const countReader = await connection.runAndReadAll(`SELECT count(*) FROM nsd_bfs_frontier`);
        const frontierSize = Number(countReader.getRows()[0]![0]);
        
        if (frontierSize === 0) {
          break;
        }

        // Clear next frontier for this layer
        await connection.run(`DELETE FROM nsd_bfs_next_frontier`);

        // Process frontier in batches to avoid memory spikes
        for (let offset = 0; offset < frontierSize; offset += BATCH_SIZE) {
          // Clear batch candidates table for reuse
          await connection.run(`DELETE FROM nsd_batch_candidates`);
          
          // Find candidates (New Nodes) -> Temp Batch Table
          await connection.run(`
            INSERT INTO nsd_batch_candidates
            SELECT DISTINCT f.followed_pubkey
            FROM (
              SELECT pubkey FROM nsd_bfs_frontier LIMIT ? OFFSET ?
            ) batch
            JOIN nsd_follows f ON batch.pubkey = f.follower_pubkey
            LEFT JOIN nsd_root_distances existing ON f.followed_pubkey = existing.pubkey
            WHERE existing.pubkey IS NULL
          `, [BATCH_SIZE, offset]);
          
          // Move candidates to permanent storage
          await connection.run(`
            INSERT INTO nsd_root_distances (pubkey, distance)
            SELECT pubkey, ? FROM nsd_batch_candidates
          `, [currentDepth + 1]);
          
          // Move candidates to next frontier
          await connection.run(`
            INSERT INTO nsd_bfs_next_frontier (pubkey)
            SELECT pubkey FROM nsd_batch_candidates
          `);
        }

        // Swap frontiers: Next becomes Current
        await connection.run(`DELETE FROM nsd_bfs_frontier`);
        await connection.run(`
          INSERT INTO nsd_bfs_frontier (pubkey)
          SELECT pubkey FROM nsd_bfs_next_frontier
        `);
      }

      // Cleanup temporary tables
      await connection.run(`DROP TABLE IF EXISTS nsd_bfs_frontier`);
      await connection.run(`DROP TABLE IF EXISTS nsd_bfs_next_frontier`);
      await connection.run(`DROP TABLE IF EXISTS nsd_batch_candidates`);

      // Create index for fast lookups (Primary Key already covers unique lookups)
      await connection.run(`
        CREATE INDEX IF NOT EXISTS idx_root_distances ON nsd_root_distances(pubkey)
      `);

      // Update metadata
      await setMetadataValue(connection, 'root_pubkey', normalizedRoot);
      await setMetadataValue(connection, 'root_depth', String(maxDepth));
      await setMetadataValue(connection, 'root_built_at', String(Date.now()));

      // Commit transaction
      await connection.run("COMMIT");
    } catch (error) {
      // Rollback on any error
      await connection.run("ROLLBACK");
      throw error;
    }
  });
}

/**
 * Updates the root distances table with delta changes from updated pubkeys
 * This implements the "Delta Patch" strategy for progressive evolution
 *
 * @param connection - Active DuckDB connection
 * @param updatedPubkeys - Array of pubkeys that had their follow lists updated
 */
export async function updateRootDistancesDelta(
  connection: DuckDBConnection,
  updatedPubkeys: string[],
): Promise<void> {
  if (updatedPubkeys.length === 0) {
    return;
  }

  // Get current root pubkey from metadata
  const rootPubkey = await getMetadataValue(connection, 'root_pubkey');
  if (!rootPubkey) {
    // No root table exists, nothing to update
    return;
  }

  // Normalize updated pubkeys
  const normalizedUpdated = updatedPubkeys.map(normalizePubkey);
  
  // Get max depth from metadata
  const rootDepthStr = await getMetadataValue(connection, 'root_depth');
  const maxDepth = rootDepthStr ? parseInt(rootDepthStr, 10) : 6;

  await executeWithRetry(async () => {
    await connection.run("BEGIN TRANSACTION");
    try {
      let frontier = [...normalizedUpdated];
      
      // Iteratively propagate updates
      // We limit iterations to avoid infinite loops, though maxDepth should naturally limit it
      for (let i = 0; i < maxDepth + 2; i++) {
        if (frontier.length === 0) break;

        // Filter frontier to avoid reprocessing in the same transaction if cycles exist
        // (though the distance check prevents cycles from propagating)
        
        // Find all nodes that need to be updated based on the current frontier
        // We use a temporary table or just select them first
        const placeholders = frontier.map(() => '?').join(',');
        
        const updatesReader = await connection.runAndReadAll(
          `
          SELECT DISTINCT
            f.followed_pubkey as pubkey,
            rd.distance + 1 as new_distance
          FROM nsd_follows f
          JOIN nsd_root_distances rd ON f.follower_pubkey = rd.pubkey
          WHERE f.follower_pubkey IN (${placeholders})
            AND rd.distance + 1 <= ? -- Respect max depth
            AND (
              -- They are new to the graph (not in distances table)
              f.followed_pubkey NOT IN (SELECT pubkey FROM nsd_root_distances)
              OR
              -- Or we found a shorter path
              rd.distance + 1 < (SELECT distance FROM nsd_root_distances WHERE pubkey = f.followed_pubkey)
            )
          `,
          [...frontier, maxDepth]
        );
        
        const updates = updatesReader.getRows();
        
        if (updates.length === 0) {
          break;
        }
        
        const nextFrontier: string[] = [];
        const updatePubkeys: string[] = [];
        
        // Collect updates
        for (const row of updates) {
          const pubkey = String(row[0]);
          // const distance = Number(row[1]);
          nextFrontier.push(pubkey);
          updatePubkeys.push(pubkey);
        }
        
        // Apply updates
        if (updatePubkeys.length > 0) {
          const updatePlaceholders = updatePubkeys.map(() => '?').join(',');
          
          // 1. Delete existing entries
          await connection.run(
            `DELETE FROM nsd_root_distances WHERE pubkey IN (${updatePlaceholders})`,
            updatePubkeys
          );
          
          // 2. Insert new entries
          // We need to re-calculate the best distance because multiple parents in frontier might point to same child
          // with different distances. We want the minimum.
          await connection.run(
            `
            INSERT INTO nsd_root_distances (pubkey, distance)
            SELECT
              f.followed_pubkey,
              MIN(rd.distance + 1)
            FROM nsd_follows f
            JOIN nsd_root_distances rd ON f.follower_pubkey = rd.pubkey
            WHERE f.followed_pubkey IN (${updatePlaceholders})
            GROUP BY f.followed_pubkey
            `,
            updatePubkeys
          );
        }
        
        frontier = nextFrontier;
      }

      // Update the build timestamp to reflect this delta update
      await connection.run(
        `INSERT OR REPLACE INTO nsd_metadata (key, value) VALUES ('root_built_at', ?)`,
        [String(Date.now())]
      );

      await connection.run("COMMIT");
    } catch (error) {
      await connection.run("ROLLBACK");
      throw error;
    }
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
  assumeSourceExists: boolean = false,
): Promise<number | null> {
  // Normalize pubkeys to lowercase for consistent comparison
  const normalizedFrom = normalizePubkey(fromPubkey);
  const normalizedTo = normalizePubkey(toPubkey);

  // If source and target are the same, return distance 0
  if (normalizedFrom === normalizedTo) {
    return 0;
  }

  // Check if source exists (unless caller assumes it exists)
  if (!assumeSourceExists) {
    if (!(await pubkeyExists(connection, normalizedFrom))) {
      return null;
    }
  }

  // Check for direct connection first
  if (await checkDirectConnection(connection, normalizedFrom, normalizedTo)) {
    return 1;
  }

  // For distance 2, use optimized query that limits exploration
  if (maxDepth >= 2) {
    const distance2Result = await checkDistance2Connection(connection, normalizedFrom, normalizedTo);
    if (distance2Result) {
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

/**
 * Gets distances from root to multiple target pubkeys using the pre-calculated table
 *
 * @param connection - Active DuckDB connection
 * @param toPubkeys - Array of target pubkeys
 * @returns Promise resolving to a map of target pubkey -> distance
 */
export async function getDistancesFromRootBatch(
  connection: DuckDBConnection,
  toPubkeys: string[],
): Promise<Map<string, number | null>> {
  if (toPubkeys.length === 0) {
    return new Map();
  }

  // Use parameterized query with IN clause for efficient batch lookup
  const placeholders = toPubkeys.map(() => '?').join(',');
  const reader = await connection.runAndReadAll(
    `SELECT pubkey, distance FROM nsd_root_distances WHERE pubkey IN (${placeholders})`,
    toPubkeys
  );

  const rows = reader.getRows();
  const result = new Map<string, number | null>();

  // Initialize result with all target pubkeys set to null
  for (const pubkey of toPubkeys) {
    result.set(pubkey, null);
  }

  // Update with actual distances from the query results
  for (const row of rows) {
    const pubkey = String(row[0]);
    const distance = Number(row[1]);
    result.set(pubkey, distance);
  }

  return result;
}

/**
 * Gets distances from a source to multiple target pubkeys using batch bidirectional search
 *
 * @param connection - Active DuckDB connection
 * @param fromPubkey - Starting pubkey
 * @param toPubkeys - Array of target pubkeys
 * @param maxDepth - Maximum search depth
 * @returns Promise resolving to a map of target pubkey -> distance
 */
export async function getDistancesBatchBidirectional(
  connection: DuckDBConnection,
  fromPubkey: string,
  toPubkeys: string[],
  maxDepth: number,
): Promise<Map<string, number | null>> {
  if (toPubkeys.length === 0) {
    return new Map();
  }

  // Normalize source pubkey once
  const normalizedFrom = normalizePubkey(fromPubkey);
  
  // Check if source pubkey exists in the graph (once for entire batch)
  if (!(await pubkeyExists(connection, normalizedFrom))) {
    // Source doesn't exist - return all nulls
    const result = new Map<string, number | null>();
    for (const pubkey of toPubkeys) {
      result.set(pubkey, null);
    }
    return result;
  }

  // For batch operations, we'll use individual queries for each target
  // This is simpler and more reliable than complex recursive CTEs with multiple targets
  const result = new Map<string, number | null>();
  
  // Initialize with null values for all targets
  for (const pubkey of toPubkeys) {
    result.set(pubkey, null);
  }

  // Process each target individually using optimized function that skips source existence check
  for (const targetPubkey of toPubkeys) {
    const distance = await findShortestDistance(connection, normalizedFrom, targetPubkey, maxDepth, true);
    result.set(targetPubkey, distance);
  }

  return result;
}
