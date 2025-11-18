/**
 * Database initialization and schema management for DuckDB
 */

import { DuckDBInstance, DuckDBConnection } from "@duckdb/node-api";

/**
 * SQL schema for the follows table
 *
 * Optimizations:
 * 1. Removed event_id from PRIMARY KEY - we only keep latest event per follower
 * 2. Removed event_id column entirely - not needed for graph analysis
 * 3. Simplified to just the edge data: follower -> followed with timestamp
 * 4. PRIMARY KEY on (follower_pubkey, followed_pubkey) provides implicit index
 */
const CREATE_FOLLOWS_TABLE = `
CREATE TABLE IF NOT EXISTS nsd_follows (
    follower_pubkey VARCHAR(64) NOT NULL,
    followed_pubkey VARCHAR(64) NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (follower_pubkey, followed_pubkey)
);
`;

/**
 * SQL to create indexes for efficient graph traversal
 *
 * Optimizations:
 * 1. Removed compound index - PRIMARY KEY already provides this
 * 2. Only need index on followed_pubkey for reverse lookups
 * 3. follower_pubkey is already indexed via PRIMARY KEY
 */
const CREATE_INDEXES = `
-- Index for finding who follows a pubkey (incoming edges)
-- The PRIMARY KEY already indexes follower_pubkey for outgoing edges
CREATE INDEX IF NOT EXISTS idx_nsd_follows_followed ON nsd_follows(followed_pubkey);
`;

/**
 * Initializes a DuckDB database instance
 * @param dbPath - Path to the database file, or ':memory:' for in-memory database
 * @returns Promise resolving to the DuckDB instance
 */
export async function initializeDatabase(
  dbPath: string = ":memory:",
): Promise<DuckDBInstance> {
  const instance = await DuckDBInstance.create(dbPath);
  return instance;
}

/**
 * Sets up the database schema (tables and indexes)
 * @param connection - Active DuckDB connection
 */
export async function setupSchema(connection: DuckDBConnection): Promise<void> {
  // Create table and indexes in a single transaction
  await connection.run(`
    BEGIN TRANSACTION;
    ${CREATE_FOLLOWS_TABLE}
    ${CREATE_INDEXES}
    COMMIT;
  `);
}

/**
 * Gets statistics about the follows table
 * @param connection - Active DuckDB connection
 * @returns Object containing table statistics
 */
export async function getTableStats(connection: DuckDBConnection): Promise<{
  totalFollows: number;
  uniqueFollowers: number;
  uniqueFollowed: number;
  uniqueEvents: number;
}> {
  const reader = await connection.runAndReadAll(`
    SELECT
      COUNT(*) as total_follows,
      COUNT(DISTINCT follower_pubkey) as unique_followers,
      COUNT(DISTINCT followed_pubkey) as unique_followed,
      COUNT(DISTINCT follower_pubkey) as unique_events
    FROM nsd_follows
  `);

  const rows = reader.getRows();

  if (rows.length === 0) {
    return {
      totalFollows: 0,
      uniqueFollowers: 0,
      uniqueFollowed: 0,
      uniqueEvents: 0,
    };
  }

  const row = rows[0];
  return {
    totalFollows: Number(row![0]),
    uniqueFollowers: Number(row![1]),
    uniqueFollowed: Number(row![2]),
    uniqueEvents: Number(row![3]),
  };
}

/**
 * Checks if a pubkey exists in the graph (either as follower or followed)
 * @param connection - Active DuckDB connection
 * @param pubkey - The pubkey to check
 * @returns Promise resolving to true if the pubkey exists
 */
export async function pubkeyExists(
  connection: DuckDBConnection,
  pubkey: string,
): Promise<boolean> {
  const reader = await connection.runAndReadAll(
    `
    SELECT 1
    FROM nsd_follows
    WHERE follower_pubkey = ? OR followed_pubkey = ?
    LIMIT 1
    `,
    [pubkey, pubkey],
  );

  return reader.getRows().length > 0;
}

/**
 * Gets all pubkeys that a given pubkey follows
 * @param connection - Active DuckDB connection
 * @param pubkey - The follower pubkey
 * @returns Promise resolving to array of followed pubkeys
 */
export async function getFollowing(
  connection: DuckDBConnection,
  pubkey: string,
): Promise<string[]> {
  const reader = await connection.runAndReadAll(
    `
    SELECT DISTINCT followed_pubkey
    FROM nsd_follows
    WHERE follower_pubkey = ?
    ORDER BY followed_pubkey
    `,
    [pubkey],
  );

  return reader.getRows().map((row) => row![0] as string);
}

/**
 * Gets all pubkeys that follow a given pubkey
 * @param connection - Active DuckDB connection
 * @param pubkey - The followed pubkey
 * @returns Promise resolving to array of follower pubkeys
 */
export async function getFollowers(
  connection: DuckDBConnection,
  pubkey: string,
): Promise<string[]> {
  const reader = await connection.runAndReadAll(
    `
    SELECT DISTINCT follower_pubkey
    FROM nsd_follows
    WHERE followed_pubkey = ?
    ORDER BY follower_pubkey
    `,
    [pubkey],
  );

  return reader.getRows().map((row) => row![0] as string);
}
