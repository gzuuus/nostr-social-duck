/**
 * Graph analysis algorithms for social network traversal
 */

import { DuckDBConnection } from "@duckdb/node-api";
import type { DuckDBArrayValue } from "@duckdb/node-api";
import type { SocialPath } from "./types.js";
import { normalizePubkey } from "./parser.js";

/**
 * Finds the shortest path between two pubkeys using DuckDB's recursive CTE with USING KEY
 *
 * This implementation uses DuckDB's USING KEY feature for optimal performance:
 * - Prevents cycles by tracking visited nodes in the path
 * - Early termination when target is reached
 * - Efficient memory usage through key-based deduplication
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
  const fromExists = await connection.runAndReadAll(
    "SELECT 1 FROM nsd_follows WHERE follower_pubkey = ? OR followed_pubkey = ? LIMIT 1",
    [normalizedFrom, normalizedFrom],
  );

  const toExists = await connection.runAndReadAll(
    "SELECT 1 FROM nsd_follows WHERE follower_pubkey = ? OR followed_pubkey = ? LIMIT 1",
    [normalizedTo, normalizedTo],
  );

  if (fromExists.getRows().length === 0 || toExists.getRows().length === 0) {
    // One or both pubkeys don't exist in the graph
    return null;
  }

  // Execute the shortest path query using recursive CTE with USING KEY
  const reader = await connection.runAndReadAll(
    `
    WITH RECURSIVE social_path(start_pubkey, end_pubkey, path, distance)
    USING KEY (end_pubkey) AS (
      -- Base case: direct follows from source pubkey
      SELECT
        follower_pubkey AS start_pubkey,
        followed_pubkey AS end_pubkey,
        [follower_pubkey, followed_pubkey] AS path,
        1 AS distance
      FROM nsd_follows
      WHERE follower_pubkey = ?
      
      UNION
      
      -- Recursive case: follow chains with cycle detection
      SELECT
        sp.start_pubkey,
        f.followed_pubkey,
        list_append(sp.path, f.followed_pubkey) AS path,
        sp.distance + 1 AS distance
      FROM social_path sp
      JOIN nsd_follows f ON sp.end_pubkey = f.follower_pubkey
      WHERE NOT list_contains(sp.path, f.followed_pubkey) -- Prevent cycles
        AND sp.distance < ?
    )
    SELECT path, distance
    FROM social_path
    WHERE end_pubkey = ?
    ORDER BY distance ASC
    LIMIT 1
    `,
    [normalizedFrom, maxDepth, normalizedTo],
  );

  const rows = reader.getRows();

  // No path found
  if (rows.length === 0) {
    return null;
  }

  const row = rows[0]!;
  const path =
    (Array.isArray(row[0])
      ? row[0]
      : (row[0] as DuckDBArrayValue | undefined)?.items) || [];
  const distance = Number(row[1]);

  return {
    path: path as string[],
    distance,
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
    SELECT COUNT(*) as count
    FROM nsd_follows f1
    JOIN nsd_follows f2 ON
      f1.follower_pubkey = f2.followed_pubkey AND
      f1.followed_pubkey = f2.follower_pubkey
    WHERE f1.follower_pubkey = ?
      AND f1.followed_pubkey = ?
    `,
    [normalized1, normalized2],
  );

  const rows = reader.getRows();
  return rows.length > 0 && Number(rows[0]![0]) > 0;
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
