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
} from "./graph-analysis.js";
import { normalizePubkey } from "./parser.js";

/**
 * DuckDB-based Social Graph Analyzer for Nostr Kind 3 events
 *
 * This class provides the main API for analyzing social graphs from Nostr follow lists.
 * It uses DuckDB for efficient graph traversal and analysis.
 *
 * @example
 * ```typescript
 * // Create an in-memory analyzer
 * const analyzer = await DuckDBSocialGraphAnalyzer.create();
 *
 * // Ingest events
 * await analyzer.ingestEvents(kind3Events);
 *
 * // Find shortest path
 * const path = await analyzer.getShortestPath(fromPubkey, toPubkey);
 *
 * // Clean up
 * await analyzer.close();
 * ```
 */
export class DuckDBSocialGraphAnalyzer implements ISocialGraphAnalyzer {
  private instance: DuckDBInstance | null = null;
  private connection: DuckDBConnection | null = null;
  private maxDepth: number;
  private closed: boolean = false;
  private rootPubkey: string | null = null;
  private rootTableValid: boolean = false;

  /**
   * Private constructor - use static create() or connect() methods instead
   */
  private constructor(
    instance: DuckDBInstance | null,
    connection: DuckDBConnection | null,
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
   * @example
   * ```typescript
   * // In-memory database
   * const analyzer = await DuckDBSocialGraphAnalyzer.create();
   *
   * // Persistent database
   * const analyzer = await DuckDBSocialGraphAnalyzer.create({
   *   dbPath: './social-graph.db'
   * });
   * ```
   */
  static async create(
    config: SocialGraphConfig = {},
  ): Promise<DuckDBSocialGraphAnalyzer> {
    const { dbPath = ":memory:", maxDepth = 6, rootPubkey } = config;

    // Initialize database
    const instance = await initializeDatabase(dbPath);

    // Create analyzer instance
    const analyzer = new DuckDBSocialGraphAnalyzer(instance, null, maxDepth);

    // Get connection and setup schema
    await analyzer.ensureConnection();
    await setupSchema(analyzer.connection!);

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
   * @example
   * ```typescript
   * // Use existing connection
   * const connection = await myInstance.connect();
   * const analyzer = await DuckDBSocialGraphAnalyzer.connect(connection);
   *
   * // Use with custom maxDepth
   * const analyzer = await DuckDBSocialGraphAnalyzer.connect(connection, 10);
   * ```
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
   * Ensures a database connection is available and valid
   * @private
   */
  private async ensureConnection(): Promise<void> {
    if (this.closed) {
      throw new Error("Analyzer has been closed");
    }

    if (!this.connection) {
      if (!this.instance) {
        throw new Error(
          "No database instance available - use connect() method for external connections",
        );
      }
      this.connection = await this.instance.connect();
    }

    // Validate connection by running a simple query
    try {
      await this.connection.run("SELECT 1");
    } catch (error) {
      // Connection is invalid, try to reconnect if we own the instance
      console.error("Database connection is invalid:", error);
      if (this.instance) {
        try {
          this.connection.closeSync();
        } catch {
          // Ignore close errors
        }
        this.connection = await this.instance.connect();
      } else {
        // External connection - rethrow the error
        throw new Error("Database connection is invalid");
      }
    }
  }

  /**
   * Ingests a single Kind 3 Nostr event into the graph
   *
   * @param event - The Nostr Kind 3 event to ingest
   * @throws Error if the event is invalid or the analyzer is closed
   *
   * @example
   * ```typescript
   * await analyzer.ingestEvent({
   *   id: "event123...",
   *   pubkey: "pubkey123...",
   *   kind: 3,
   *   created_at: 1234567890,
   *   tags: [
   *     ["p", "followed_pubkey1..."],
   *     ["p", "followed_pubkey2..."]
   *   ],
   *   content: "",
   *   sig: "signature..."
   * });
   * ```
   */
  async ingestEvent(event: NostrEvent): Promise<void> {
    await this.ensureConnection();
    await ingestSingleEvent(this.connection!, event);

    // Mark root table as invalid since the graph has changed
    if (this.rootPubkey) {
      this.rootTableValid = false;
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
   * @example
   * ```typescript
   * await analyzer.ingestEvents([event1, event2, event3]);
   * ```
   */
  async ingestEvents(events: NostrEvent[]): Promise<void> {
    await this.ensureConnection();
    await ingestMultipleEvents(this.connection!, events);

    // Mark root table as invalid since the graph has changed
    if (this.rootPubkey) {
      this.rootTableValid = false;
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
   * @example
   * ```typescript
   * const path = await analyzer.getShortestPath(
   *   "abc123...",
   *   "def456..."
   * );
   *
   * if (path) {
   *   console.log(`Distance: ${path.distance}`);
   *   console.log(`Path: ${path.path.join(' -> ')}`);
   * }
   * ```
   */
  async getShortestPath(
    fromPubkey: string,
    toPubkey: string,
    maxDepth?: number,
  ): Promise<SocialPath | null> {
    await this.ensureConnection();
    const depth = maxDepth ?? this.maxDepth;
    return findShortestPath(this.connection!, fromPubkey, toPubkey, depth);
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
   * @example
   * ```typescript
   * const distance = await analyzer.getShortestDistance(
   *   "abc123...",
   *   "def456..."
   * );
   *
   * if (distance !== null) {
   *   console.log(`Distance: ${distance} hops`);
   * }
   * ```
   */
  async getShortestDistance(
    fromPubkey: string,
    toPubkey: string,
    maxDepth?: number,
  ): Promise<number | null> {
    await this.ensureConnection();
    const depth = maxDepth ?? this.maxDepth;
    const normalizedFrom = normalizePubkey(fromPubkey);

    // If fromPubkey matches the rootPubkey, use the optimized path
    if (this.rootPubkey && normalizedFrom === this.rootPubkey) {
      // Rebuild table if invalid
      if (!this.rootTableValid) {
        await buildRootDistancesTable(
          this.connection!,
          this.rootPubkey,
          this.maxDepth,
        );
        this.rootTableValid = true;
      }
      return getDistanceFromRoot(this.connection!, toPubkey);
    }

    // Otherwise, use the standard bidirectional search
    return findShortestDistance(this.connection!, fromPubkey, toPubkey, depth);
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
   * @example
   * ```typescript
   * // Get all users within 2 hops from a pubkey
   * const nearbyUsers = await analyzer.getUsersWithinDistance(
   *   "abc123...",
   *   2
   * );
   * // Returns: ["def456...", "ghi789...", ...]
   * ```
   */
  async getUsersWithinDistance(
    fromPubkey: string,
    distance: number,
  ): Promise<string[] | null> {
    await this.ensureConnection();
    const normalizedFrom = normalizePubkey(fromPubkey);

    // Hybrid approach: Use optimized table if querying from root pubkey
    if (this.rootPubkey && normalizedFrom === this.rootPubkey) {
      // Rebuild table if invalid
      if (!this.rootTableValid) {
        await buildRootDistancesTable(
          this.connection!,
          this.rootPubkey,
          this.maxDepth,
        );
        this.rootTableValid = true;
      }
      return getUsersWithinDistanceFromRoot(this.connection!, distance);
    }

    // Fallback to standard recursive CTE for other pubkeys
    return getUsersWithinDistance(this.connection!, fromPubkey, distance);
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
    await this.ensureConnection();

    if (!this.rootPubkey) {
      throw new Error("Root pubkey must be set to use getUsersAtDistance");
    }

    // Rebuild table if invalid
    if (!this.rootTableValid) {
      await buildRootDistancesTable(
        this.connection!,
        this.rootPubkey,
        this.maxDepth,
      );
      this.rootTableValid = true;
    }

    return getUsersAtDistanceFromRoot(this.connection!, distance);
  }

  /**
   * Get the distribution of users by distance from the root pubkey
   *
   * This method requires a root pubkey to be set.
   *
   * @returns Promise resolving to a map of distance -> count
   */
  async getDistanceDistribution(): Promise<Record<number, number>> {
    await this.ensureConnection();

    if (!this.rootPubkey) {
      throw new Error("Root pubkey must be set to use getDistanceDistribution");
    }

    // Rebuild table if invalid
    if (!this.rootTableValid) {
      await buildRootDistancesTable(
        this.connection!,
        this.rootPubkey,
        this.maxDepth,
      );
      this.rootTableValid = true;
    }

    return getRootDistanceDistribution(this.connection!);
  }

  /**
   * Gets all unique pubkeys in the social graph (both followers and followed)
   *
   * This method efficiently retrieves all pubkeys that appear in the graph,
   * either as followers (outgoing edges) or followed (incoming edges).
   *
   * @returns Promise resolving to array of all unique pubkeys in the graph
   *
   * @example
   * ```typescript
   * const allPubkeys = await analyzer.getAllUniquePubkeys();
   * console.log(`Total unique pubkeys in graph: ${allPubkeys.length}`);
   * ```
   */
  async getAllUniquePubkeys(): Promise<string[]> {
    await this.ensureConnection();
    return getAllUniquePubkeys(this.connection!);
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
    await this.ensureConnection();
    return getTableStats(this.connection!);
  }

  /**
   * Checks if a pubkey exists in the graph (either as follower or followed)
   *
   * @param pubkey - The pubkey to check
   * @returns Promise resolving to true if the pubkey exists
   *
   * @example
   * ```typescript
   * const exists = await analyzer.pubkeyExists("abc123...");
   * console.log(`Pubkey exists in graph: ${exists}`);
   * ```
   */
  async pubkeyExists(pubkey: string): Promise<boolean> {
    await this.ensureConnection();
    return pubkeyExists(this.connection!, pubkey);
  }

  /**
   * Checks if a direct follow relationship exists between two pubkeys
   *
   * @param followerPubkey - The follower pubkey
   * @param followedPubkey - The followed pubkey
   * @returns Promise resolving to true if the relationship exists
   *
   * @example
   * ```typescript
   * const isFollowing = await analyzer.isDirectFollow(
   *   "followerPubkey...",
   *   "followedPubkey..."
   * );
   * console.log(`Direct follow exists: ${isFollowing}`);
   * ```
   */
  async isDirectFollow(
    followerPubkey: string,
    followedPubkey: string,
  ): Promise<boolean> {
    await this.ensureConnection();
    return isDirectFollow(this.connection!, followerPubkey, followedPubkey);
  }

  /**
   * Checks if two pubkeys mutually follow each other
   *
   * @param pubkey1 - First pubkey
   * @param pubkey2 - Second pubkey
   * @returns Promise resolving to true if they mutually follow each other
   *
   * @example
   * ```typescript
   * const areMutual = await analyzer.areMutualFollows(
   *   "pubkey1...",
   *   "pubkey2..."
   * );
   * console.log(`Mutual follows: ${areMutual}`);
   * ```
   */
  async areMutualFollows(pubkey1: string, pubkey2: string): Promise<boolean> {
    await this.ensureConnection();
    return areMutualFollows(this.connection!, pubkey1, pubkey2);
  }

  /**
   * Gets the degree (number of follows) for a pubkey
   *
   * @param pubkey - The pubkey to check
   * @returns Promise resolving to object with outDegree (following) and inDegree (followers)
   *
   * @example
   * ```typescript
   * const degree = await analyzer.getPubkeyDegree("pubkey123...");
   * console.log(`Following: ${degree.outDegree}, Followers: ${degree.inDegree}`);
   * ```
   */
  async getPubkeyDegree(
    pubkey: string,
  ): Promise<{ outDegree: number; inDegree: number }> {
    await this.ensureConnection();
    return getPubkeyDegree(this.connection!, pubkey);
  }

  /**
   * Closes the database connection and cleans up resources
   *
   * After calling this method, the analyzer cannot be used anymore.
   *
   * @example
   * ```typescript
   * await analyzer.close();
   * ```
   */
  async close(): Promise<void> {
    if (this.closed) {
      return;
    }

    // Clear the root distances table
    if (this.rootPubkey) {
      await this.clearRootDistances();
    }

    // Reclaim space before closing - only when we own the connection
    // CHECKPOINT is only called when we created the database instance ourselves
    // to avoid conflicts with other transactions in external projects
    if (this.connection && this.instance) {
      await this.connection.run("CHECKPOINT");
    }

    // Only close connection if we own it (created via create(), not connect())
    if (this.connection && this.instance) {
      this.connection.closeSync();
      this.connection = null;
    }

    this.closed = true;
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
   * @param maxDepth - The new maximum depth (must be positive)
   */
  setMaxDepth(maxDepth: number): void {
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
    await this.ensureConnection();
    const normalizedPubkey = normalizePubkey(pubkey);
    this.rootPubkey = normalizedPubkey;

    // Build the temporary table with pre-calculated distances
    await buildRootDistancesTable(
      this.connection!,
      normalizedPubkey,
      this.maxDepth,
    );
    this.rootTableValid = true;
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
   * Clears the temporary root distances table
   * @private
   */
  private async clearRootDistances(): Promise<void> {
    if (this.connection) {
      try {
        await this.connection.run(`
          DROP TABLE IF EXISTS nsd_root_distances
        `);
      } catch (error) {
        console.error("Error clearing root distances table:", error);
        // Ignore errors if table doesn't exist
      }
    }
    this.rootPubkey = null;
    this.rootTableValid = false;
  }
}
