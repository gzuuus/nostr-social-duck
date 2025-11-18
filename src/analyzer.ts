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
} from "./graph-analysis.js";

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
    const { dbPath = ":memory:", maxDepth = 6 } = config;

    // Initialize database
    const instance = await initializeDatabase(dbPath);

    // Create analyzer instance
    const analyzer = new DuckDBSocialGraphAnalyzer(instance, null, maxDepth);

    // Get connection and setup schema
    await analyzer.ensureConnection();
    await setupSchema(analyzer.connection!);

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
  ): Promise<DuckDBSocialGraphAnalyzer> {
    // Create analyzer instance with external connection
    const analyzer = new DuckDBSocialGraphAnalyzer(null, connection, maxDepth);

    // Setup schema on the external connection
    await setupSchema(connection);

    return analyzer;
  }

  /**
   * Ensures a database connection is available
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
    return getUsersWithinDistance(this.connection!, fromPubkey, distance);
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

    // Reclaim space before closing
    if (this.connection) {
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
}
