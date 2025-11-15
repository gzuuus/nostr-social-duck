/**
 * Database initialization and schema management for DuckDB
 */

import { DuckDBInstance, DuckDBConnection } from "@duckdb/node-api";

/**
 * SQL schema for the follows table
 */
const CREATE_FOLLOWS_TABLE = `
CREATE TABLE IF NOT EXISTS follows (
    follower_pubkey VARCHAR(64) NOT NULL,
    followed_pubkey VARCHAR(64) NOT NULL,
    event_id VARCHAR(64) NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (follower_pubkey, followed_pubkey, event_id)
);
`;

/**
 * SQL to create indexes for efficient graph traversal
 */
const CREATE_INDEXES = `
-- Index for finding who a pubkey follows (outgoing edges)
CREATE INDEX IF NOT EXISTS idx_follows_follower ON follows(follower_pubkey);

-- Index for finding who follows a pubkey (incoming edges)
CREATE INDEX IF NOT EXISTS idx_follows_followed ON follows(followed_pubkey);

-- Compound index for efficient lookups
CREATE INDEX IF NOT EXISTS idx_follows_compound ON follows(follower_pubkey, followed_pubkey);
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
  // Create the follows table
  await connection.run(CREATE_FOLLOWS_TABLE);

  // Create indexes for performance
  await connection.run(CREATE_INDEXES);
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
      COUNT(DISTINCT event_id) as unique_events
    FROM follows
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
    FROM follows
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
    FROM follows
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
    FROM follows
    WHERE followed_pubkey = ?
    ORDER BY follower_pubkey
    `,
    [pubkey],
  );

  return reader.getRows().map((row) => row![0] as string);
}
