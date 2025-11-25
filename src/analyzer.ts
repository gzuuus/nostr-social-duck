/**
 * Main SocialGraphAnalyzer class - the primary API for the library
 */

import { DuckDBInstance, DuckDBConnection } from "@duckdb/node-api";
import type {
  NostrEvent,
  SocialPath,
  SocialGraphConfig,
  GraphStats,
  SocialGraphAnalyzer as ISocialGraphAnalyzer,
} from "./types.js";
import {
  initializeDatabase,
  setupSchema,
  getTableStats,
  pubkeyExists,
} from "./database.js";
import {
  ingestEvent as ingestSingleEvent,
  ingestEvents as ingestMultipleEvents,
} from "./ingestion.js";
import {
  findShortestPath,
  findShortestDistance,
  getUsersWithinDistance,
  getAllUniquePubkeys,
  buildRootDistancesTable,
  getDistanceFromRoot,
  isDirectFollow,
  areMutualFollows,
  getPubkeyDegree,
  getUsersAtDistanceFromRoot,
  getUsersWithinDistanceFromRoot,
  getRootDistanceDistribution,
  getDistancesFromRootBatch,
  getDistancesBatchBidirectional,
  updateRootDistancesDelta,
  getMetadataValue,
  setMetadataValue,
} from "./graph-analysis.js";
import { normalizePubkey } from "./parser.js";
import { executeWithRetry } from "./utils.js";

/**
 * DuckDB-based Social Graph Analyzer for Nostr Kind 3 events
 *
 * This class provides the main API for analyzing social graphs from Nostr follow lists.
 * It uses DuckDB for efficient graph traversal and analysis.
 */
export class DuckDBSocialGraphAnalyzer
  implements ISocialGraphAnalyzer, AsyncDisposable
{
  private instance: DuckDBInstance | null = null;
  private connection: DuckDBConnection;
  private maxDepth: number;
  private closed: boolean = false;
  private rootPubkey: string | null = null;
  private rootTableValid: boolean = false;

  /**
   * Private constructor - use static create() or connect() methods instead
   */
  private constructor(
    instance: DuckDBInstance | null,
    connection: DuckDBConnection,
    maxDepth: number,
  ) {
    this.instance = instance;
    this.connection = connection;
    this.maxDepth = maxDepth;
  }

  /**
   * Creates a new SocialGraphAnalyzer instance
   *
   * @param config - Configuration options
   * @returns Promise resolving to a new analyzer instance
   *
   */
  static async create(
    config: SocialGraphConfig = {},
  ): Promise<DuckDBSocialGraphAnalyzer> {
    const { dbPath = ":memory:", maxDepth = 6, rootPubkey } = config;

    // Initialize database
    const instance = await initializeDatabase(dbPath);

    // Get connection first
    const connection = await instance.connect();

    // Create analyzer instance with established connection
    const analyzer = new DuckDBSocialGraphAnalyzer(
      instance,
      connection,
      maxDepth,
    );

    // Setup schema
    await setupSchema(connection);

    // Set root pubkey if provided
    if (rootPubkey) {
      await analyzer.setRootPubkey(rootPubkey);
    }

    return analyzer;
  }

  /**
   * Creates a new SocialGraphAnalyzer instance from an existing DuckDB connection
   *
   * This method allows using the library with an existing DuckDB connection,
   * enabling integration with applications that already use DuckDB for other purposes.
   *
   * @param connection - Existing DuckDB connection
   * @param maxDepth - Maximum search depth for paths (default: 6)
   * @returns Promise resolving to a new analyzer instance
   *
   */
  static async connect(
    connection: DuckDBConnection,
    maxDepth: number = 6,
    rootPubkey?: string,
  ): Promise<DuckDBSocialGraphAnalyzer> {
    // Create analyzer instance with external connection
    const analyzer = new DuckDBSocialGraphAnalyzer(null, connection, maxDepth);

    // Setup schema on the external connection
    await setupSchema(connection);

    // Set root pubkey if provided
    if (rootPubkey) {
      await analyzer.setRootPubkey(rootPubkey);
    }

    return analyzer;
  }

  /**
   * Ingests a single Kind 3 Nostr event into the graph
   *
   * @param event - The Nostr Kind 3 event to ingest
   * @throws Error if the event is invalid or the analyzer is closed
   */
  async ingestEvent(event: NostrEvent): Promise<void> {
    if (this.closed) {
      throw new Error("Analyzer has been closed");
    }
    await ingestSingleEvent(this.connection, event);

    // Update root distances with delta if table is valid
    if (this.rootPubkey && this.rootTableValid) {
      try {
        await updateRootDistancesDelta(this.connection, [event.pubkey]);
      } catch (error) {
        console.error(
          "Delta update failed, marking root table as invalid:",
          error,
        );
        this.rootTableValid = false;
      }
    }
  }

  /**
   * Ingests multiple Kind 3 Nostr events into the graph
   *
   * Events are deduplicated by pubkey, keeping only the latest event
   * for each pubkey based on the created_at timestamp.
   *
   * @param events - Array of Nostr Kind 3 events to ingest
   * @throws Error if any event is invalid or the analyzer is closed
   *
   */
  async ingestEvents(events: NostrEvent[]): Promise<void> {
    if (this.closed) {
      throw new Error("Analyzer has been closed");
    }
    await ingestMultipleEvents(this.connection, events);

    // Update root distances with delta if table is valid
    if (this.rootPubkey && this.rootTableValid) {
      try {
        // Get unique pubkeys that were updated (deduplicate by pubkey)
        const updatedPubkeys = [
          ...new Set(events.map((event) => event.pubkey)),
        ];
        await updateRootDistancesDelta(this.connection, updatedPubkeys);
      } catch (error) {
        console.error(
          "Delta update failed, marking root table as invalid:",
          error,
        );
        this.rootTableValid = false;
      }
    }
  }

  /**
   * Finds the shortest path between two pubkeys in the social graph
   *
   * Uses DuckDB's recursive CTE with USING KEY for efficient traversal.
   * Returns null if no path exists within the maximum depth.
   *
   * @param fromPubkey - Starting pubkey (64-character hex string)
   * @param toPubkey - Target pubkey (64-character hex string)
   * @param maxDepth - Maximum search depth (defaults to analyzer's maxDepth)
   * @returns Promise resolving to the shortest path, or null if no path exists
   *
   */
  async getShortestPath(
    fromPubkey: string,
    toPubkey: string,
    maxDepth?: number,
  ): Promise<SocialPath | null> {
    if (this.closed) {
      throw new Error("Analyzer has been closed");
    }
    const depth = maxDepth ?? this.maxDepth;
    return findShortestPath(this.connection, fromPubkey, toPubkey, depth);
  }

  /**
   * Finds the shortest distance between two pubkeys in the social graph
   *
   * This is a performance-optimized version that only returns the distance,
   * skipping the expensive path reconstruction. It's 2-3x faster than getShortestPath.
   *
   * @param fromPubkey - Starting pubkey (64-character hex string)
   * @param toPubkey - Target pubkey (64-character hex string)
   * @param maxDepth - Maximum search depth (defaults to analyzer's maxDepth)
   * @returns Promise resolving to the distance, or null if no path exists
   *
   */
  async getShortestDistance(
    fromPubkey: string,
    toPubkey: string,
    maxDepth?: number,
  ): Promise<number | null> {
    if (this.closed) {
      throw new Error("Analyzer has been closed");
    }
    const depth = maxDepth ?? this.maxDepth;
    const normalizedFrom = normalizePubkey(fromPubkey);

    // If fromPubkey matches the rootPubkey, use the optimized path
    if (this.rootPubkey && normalizedFrom === this.rootPubkey) {
      // Rebuild table if invalid
      if (!this.rootTableValid) {
        await buildRootDistancesTable(
          this.connection,
          this.rootPubkey,
          this.maxDepth,
        );
        this.rootTableValid = true;
      }
      return getDistanceFromRoot(this.connection, toPubkey);
    }

    // Otherwise, use the standard bidirectional search
    return findShortestDistance(this.connection, fromPubkey, toPubkey, depth);
  }

  /**
   * Gets the shortest distances from a source pubkey to multiple target pubkeys in batch
   *
   * This method optimizes distance calculations by leveraging batch operations,
   * which can be particularly efficient when a root is specified and the root table is available.
   * It handles both optimized root-based lookups and standard bidirectional search for non-root queries.
   *
   * @param fromPubkey - Starting pubkey (64-character hex string)
   * @param toPubkeys - Array of target pubkeys to calculate distances to
   * @param maxDepth - Maximum search depth (defaults to analyzer's maxDepth)
   * @returns Promise resolving to a map of target pubkey -> distance, or null if no path exists
   *
   */
  async getShortestDistancesBatch(
    fromPubkey: string,
    toPubkeys: string[],
  ): Promise<Map<string, number | null>> {
    if (this.closed) {
      throw new Error("Analyzer has been closed");
    }
    const normalizedFrom = normalizePubkey(fromPubkey);

    if (this.rootPubkey && normalizedFrom === this.rootPubkey) {
      if (!this.rootTableValid) {
        await buildRootDistancesTable(
          this.connection,
          this.rootPubkey,
          this.maxDepth,
        );
        this.rootTableValid = true;
      }
      return getDistancesFromRootBatch(this.connection, toPubkeys);
    }

    return getDistancesBatchBidirectional(
      this.connection,
      fromPubkey,
      toPubkeys,
      this.maxDepth,
    );
  }

  /**
   * Gets all pubkeys reachable from a starting pubkey within a specified distance
   *
   * Uses DuckDB's recursive CTE with USING KEY to efficiently traverse the graph
   * and collect all unique pubkeys within the distance limit.
   *
   * @param fromPubkey - Starting pubkey (64-character hex string)
   * @param distance - Maximum distance (number of hops) to search
   * @returns Promise resolving to array of pubkeys (excluding the starting pubkey)
   *
   */
  async getUsersWithinDistance(
    fromPubkey: string,
    distance: number,
  ): Promise<string[] | null> {
    if (this.closed) {
      throw new Error("Analyzer has been closed");
    }
    const normalizedFrom = normalizePubkey(fromPubkey);

    // Hybrid approach: Use optimized table if querying from root pubkey
    if (this.rootPubkey && normalizedFrom === this.rootPubkey) {
      // Rebuild table if invalid
      if (!this.rootTableValid) {
        await buildRootDistancesTable(
          this.connection,
          this.rootPubkey,
          this.maxDepth,
        );
        this.rootTableValid = true;
      }
      return getUsersWithinDistanceFromRoot(this.connection, distance);
    }

    // Fallback to standard recursive CTE for other pubkeys
    return getUsersWithinDistance(this.connection, fromPubkey, distance);
  }

  /**
   * Get all users exactly at a specific distance from the root pubkey
   *
   * This method requires a root pubkey to be set.
   *
   * @param distance - The exact distance in hops
   * @returns Promise resolving to array of pubkeys
   */
  async getUsersAtDistance(distance: number): Promise<string[]> {
    if (this.closed) {
      throw new Error("Analyzer has been closed");
    }

    if (!this.rootPubkey) {
      throw new Error("Root pubkey must be set to use getUsersAtDistance");
    }

    // Rebuild table if invalid
    if (!this.rootTableValid) {
      await buildRootDistancesTable(
        this.connection,
        this.rootPubkey,
        this.maxDepth,
      );
      this.rootTableValid = true;
    }

    return getUsersAtDistanceFromRoot(this.connection, distance);
  }

  /**
   * Get the distribution of users by distance from the root pubkey
   *
   * This method requires a root pubkey to be set.
   *
   * @returns Promise resolving to a map of distance -> count
   */
  async getDistanceDistribution(): Promise<Record<number, number>> {
    if (this.closed) {
      throw new Error("Analyzer has been closed");
    }

    if (!this.rootPubkey) {
      throw new Error("Root pubkey must be set to use getDistanceDistribution");
    }

    // Rebuild table if invalid
    if (!this.rootTableValid) {
      await buildRootDistancesTable(
        this.connection,
        this.rootPubkey,
        this.maxDepth,
      );
      this.rootTableValid = true;
    }

    return getRootDistanceDistribution(this.connection);
  }

  /**
   * Gets all unique pubkeys in the social graph (both followers and followed)
   *
   * This method efficiently retrieves all pubkeys that appear in the graph,
   * either as followers (outgoing edges) or followed (incoming edges).
   *
   * @returns Promise resolving to array of all unique pubkeys in the graph
   *
   */
  async getAllUniquePubkeys(): Promise<string[]> {
    if (this.closed) {
      throw new Error("Analyzer has been closed");
    }
    return getAllUniquePubkeys(this.connection);
  }

  /**
   * Gets statistics about the current social graph
   *
   * @returns Promise resolving to graph statistics
   *
   * @example
   * ```typescript
   * const stats = await analyzer.getStats();
   * console.log(`Total follows: ${stats.totalFollows}`);
   * console.log(`Unique users: ${stats.uniqueFollowers}`);
   * ```
   */
  async getStats(): Promise<GraphStats> {
    if (this.closed) {
      throw new Error("Analyzer has been closed");
    }
    return getTableStats(this.connection);
  }

  /**
   * Checks if a pubkey exists in the graph (either as follower or followed)
   *
   * @param pubkey - The pubkey to check
   * @returns Promise resolving to true if the pubkey exists
   *
   */
  async pubkeyExists(pubkey: string): Promise<boolean> {
    if (this.closed) {
      throw new Error("Analyzer has been closed");
    }
    return pubkeyExists(this.connection, pubkey);
  }

  /**
   * Checks if a direct follow relationship exists between two pubkeys
   *
   * @param followerPubkey - The follower pubkey
   * @param followedPubkey - The followed pubkey
   * @returns Promise resolving to true if the relationship exists
   *
   */
  async isDirectFollow(
    followerPubkey: string,
    followedPubkey: string,
  ): Promise<boolean> {
    if (this.closed) {
      throw new Error("Analyzer has been closed");
    }
    return isDirectFollow(this.connection, followerPubkey, followedPubkey);
  }

  /**
   * Checks if two pubkeys mutually follow each other
   *
   * @param pubkey1 - First pubkey
   * @param pubkey2 - Second pubkey
   * @returns Promise resolving to true if they mutually follow each other
   *
   */
  async areMutualFollows(pubkey1: string, pubkey2: string): Promise<boolean> {
    if (this.closed) {
      throw new Error("Analyzer has been closed");
    }
    return areMutualFollows(this.connection, pubkey1, pubkey2);
  }

  /**
   * Gets the degree (number of follows) for a pubkey
   *
   * @param pubkey - The pubkey to check
   * @returns Promise resolving to object with outDegree (following) and inDegree (followers)
   *
   */
  async getPubkeyDegree(
    pubkey: string,
  ): Promise<{ outDegree: number; inDegree: number }> {
    if (this.closed) {
      throw new Error("Analyzer has been closed");
    }
    return getPubkeyDegree(this.connection, pubkey);
  }

  /**
   * Closes the database connection and cleans up resources
   *
   * After calling this method, the analyzer cannot be used anymore.
   *
   */
  async close(): Promise<void> {
    if (this.closed) {
      return;
    }

    // Note: We no longer automatically clear the root distances table
    // The table is now persistent and must be explicitly dropped if needed

    // Reclaim space before closing - only when we own the connection
    // CHECKPOINT is only called when we created the database instance ourselves
    // to avoid conflicts with other transactions in external projects
    if (this.instance) {
      await executeWithRetry(async () => {
        await this.connection.run("CHECKPOINT");
      });
      this.connection.closeSync();
    }

    this.closed = true;
  }

  /**
   * Implementation of AsyncDisposable for 'await using' syntax
   */
  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  /**
   * Checks if the analyzer has been closed
   *
   * @returns true if the analyzer is closed
   */
  isClosed(): boolean {
    return this.closed;
  }

  /**
   * Gets the configured maximum depth for path searches
   *
   * @returns The maximum depth value
   */
  getMaxDepth(): number {
    return this.maxDepth;
  }

  /**
   * Sets a new maximum depth for path searches
   *
   * @param maxDepth - The new maximum depth (defaults to 6 if undefined/null)
   */
  setMaxDepth(maxDepth: number): void {
    // Handle undefined/null by defaulting to 6
    if (maxDepth === undefined || maxDepth === null) {
      this.maxDepth = 6;
      return;
    }
    if (typeof maxDepth !== "number") {
      throw new Error("maxDepth must be a number");
    }
    if (maxDepth < 1) {
      throw new Error("maxDepth must be at least 1");
    }
    this.maxDepth = maxDepth;
  }

  /**
   * Sets the root pubkey for optimized distance calculations.
   *
   * This pre-calculates distances from the root pubkey to all other nodes
   * using a temporary table, making subsequent getShortestDistance calls
   * from this pubkey extremely fast (O(1)).
   *
   * @param pubkey - The root pubkey to optimize for
   */
  async setRootPubkey(pubkey: string): Promise<void> {
    if (this.closed) {
      throw new Error("Analyzer has been closed");
    }
    const normalizedPubkey = normalizePubkey(pubkey);

    // Check if we can reuse the existing table
    const existingRootPubkey = await getMetadataValue(
      this.connection,
      "root_pubkey",
    );
    const existingRootDepth = await getMetadataValue(
      this.connection,
      "root_depth",
    );
    const tableExists = await this.rootTableExists();

    if (
      existingRootPubkey === normalizedPubkey &&
      existingRootDepth === String(this.maxDepth) &&
      tableExists
    ) {
      // Reuse existing table
      this.rootPubkey = normalizedPubkey;
      this.rootTableValid = true;
    } else {
      // Build new table
      this.rootPubkey = normalizedPubkey;
      await buildRootDistancesTable(
        this.connection,
        normalizedPubkey,
        this.maxDepth,
      );
      this.rootTableValid = true;
    }
  }

  /**
   * Gets the currently configured root pubkey
   *
   * @returns The root pubkey or null if not set
   */
  getRootPubkey(): string | null {
    return this.rootPubkey;
  }

  /**
   * Checks if the root distances table exists
   * @private
   */
  private async rootTableExists(): Promise<boolean> {
    try {
      const reader = await this.connection.runAndReadAll(
        "SELECT 1 FROM information_schema.tables WHERE table_name = 'nsd_root_distances'",
      );
      return reader.getRows().length > 0;
    } catch (error) {
      console.error("Error checking root distances table:", error);
      return false;
    }
  }

  /**
   * Rebuilds the root distances table from scratch
   * Useful when you want to ensure the table is completely up-to-date
   */
  async rebuildRootDistances(): Promise<void> {
    if (this.closed) {
      throw new Error("Analyzer has been closed");
    }
    if (!this.rootPubkey) {
      throw new Error("Root pubkey must be set to rebuild root distances");
    }

    await buildRootDistancesTable(
      this.connection,
      this.rootPubkey,
      this.maxDepth,
    );
    this.rootTableValid = true;
  }

  /**
   * Drops the root distances table explicitly
   * This is now an explicit operation instead of automatic cleanup
   */
  async dropRootDistances(): Promise<void> {
    if (this.closed) {
      throw new Error("Analyzer has been closed");
    }

    try {
      await executeWithRetry(async () => {
        await this.connection.run("DROP TABLE IF EXISTS nsd_root_distances");
      });
    } catch (error) {
      console.error("Error dropping root distances table:", error);
      // Ignore errors if table doesn't exist
    }

    // Clear metadata
    await setMetadataValue(this.connection, "root_pubkey", "");
    await setMetadataValue(this.connection, "root_depth", "");
    await setMetadataValue(this.connection, "root_built_at", "");

    this.rootPubkey = null;
    this.rootTableValid = false;
  }
}
