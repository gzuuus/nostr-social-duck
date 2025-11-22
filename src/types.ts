/**
 * Type definitions for Nostr Social Graph Analysis Library
 */

/**
 * Nostr Event structure following NIP-01
 */
export interface NostrEvent {
  /** 32-byte lowercase hex-encoded sha256 of the serialized event data */
  id: string;
  /** 32-byte lowercase hex-encoded public key of the event creator */
  pubkey: string;
  /** Unix timestamp in seconds */
  created_at: number;
  /** Integer between 0 and 65535 */
  kind: number;
  /** Array of tags, each tag is an array of strings */
  tags: string[][];
  /** Arbitrary string */
  content: string;
  /** 64-byte lowercase hex of the signature of the sha256 hash of the serialized event data */
  sig: string;
}

/**
 * Parsed follow relationship from a Kind 3 event
 */
export interface FollowRelationship {
  /** The pubkey of the person doing the following (event author) */
  follower_pubkey: string;
  /** The pubkey being followed */
  followed_pubkey: string;
  /** Unix timestamp when this relationship was recorded */
  created_at: number;
}

/**
 * Result of parsing a Kind 3 event
 */
export interface ParsedKind3Event {
  /** Array of follow relationships extracted from the event */
  follows: FollowRelationship[];
  /** The original event */
  event: NostrEvent;
}

/**
 * Represents a path through the social graph
 */
export interface SocialPath {
  /** Array of pubkeys representing the path from start to end */
  path: string[];
  /** The distance (number of hops) in the path */
  distance: number;
}

/**
 * Configuration options for the SocialGraphAnalyzer
 */
export interface SocialGraphConfig {
  /** Path to the DuckDB database file. Use ':memory:' for in-memory database */
  dbPath?: string;
  /** Maximum depth for shortest path searches (default: 6) */
  maxDepth?: number;
  /** Root pubkey for optimized distance calculations (optional) */
  rootPubkey?: string;
}

/**
 * Statistics about the social graph
 */
export interface GraphStats {
  /** Total number of follow relationships */
  totalFollows: number;
  /** Number of unique followers */
  uniqueFollowers: number;
  /** Number of unique followed pubkeys */
  uniqueFollowed: number;
  /** Number of unique events processed */
  uniqueEvents: number;
}

/**
 * Main interface for the Social Graph Analyzer
 */
export interface SocialGraphAnalyzer {
  /**
   * Ingest a single Kind 3 Nostr event into the graph
   * @param event - The Nostr Kind 3 event to ingest
   * @returns Promise that resolves when ingestion is complete
   */
  ingestEvent(event: NostrEvent): Promise<void>;

  /**
   * Ingest multiple Kind 3 Nostr events into the graph
   * @param events - Array of Nostr Kind 3 events to ingest
   * @returns Promise that resolves when all events are ingested
   */
  ingestEvents(events: NostrEvent[]): Promise<void>;

  /**
   * Find the shortest path between two pubkeys in the social graph
   * @param fromPubkey - Starting pubkey (64-character hex string)
   * @param toPubkey - Target pubkey (64-character hex string)
   * @param maxDepth - Maximum search depth (default: 6)
   * @returns Promise resolving to the shortest path, or null if no path exists
   */
  getShortestPath(
    fromPubkey: string,
    toPubkey: string,
    maxDepth?: number,
  ): Promise<SocialPath | null>;

  /**
   * Find the shortest distance between two pubkeys in the social graph
   * This is a performance-optimized version that only returns the distance,
   * skipping the expensive path reconstruction. It's 2-3x faster than getShortestPath.
   * @param fromPubkey - Starting pubkey (64-character hex string)
   * @param toPubkey - Target pubkey (64-character hex string)
   * @param maxDepth - Maximum search depth (default: 6)
   * @returns Promise resolving to the distance, or null if no path exists
   */
  getShortestDistance(
    fromPubkey: string,
    toPubkey: string,
    maxDepth?: number,
  ): Promise<number | null>;

  /**
   * Gets the shortest distances from a source pubkey to multiple target pubkeys in batch
   *
   * This method optimizes distance calculations by leveraging batch operations,
   * which can be particularly efficient when a root is specified and the root table is available.
   *
   * @param fromPubkey - Starting pubkey (64-character hex string)
   * @param toPubkeys - Array of target pubkeys to calculate distances to
   * @param maxDepth - Maximum search depth (default: 6)
   * @returns Promise resolving to a map of target pubkey -> distance, or null if no path exists
   */
  getShortestDistancesBatch(
    fromPubkey: string,
    toPubkeys: string[],
    maxDepth?: number,
  ): Promise<Map<string, number | null>>;

  /**
   * Get all pubkeys reachable from a starting pubkey within a specified distance
   * @param fromPubkey - Starting pubkey (64-character hex string)
   * @param distance - Maximum distance (number of hops) to search
   * @returns Promise resolving to array of pubkeys (excluding the starting pubkey)
   */
  getUsersWithinDistance(
    fromPubkey: string,
    distance: number,
  ): Promise<string[] | null>;

  /**
   * Get all unique pubkeys in the social graph (both followers and followed)
   * @returns Promise resolving to array of all unique pubkeys in the graph
   */
  getAllUniquePubkeys(): Promise<string[]>;

  /**
   * Get statistics about the current social graph
   * @returns Promise resolving to graph statistics
   */
  getStats(): Promise<GraphStats>;

  /**
   * Check if a pubkey exists in the graph (either as follower or followed)
   * @param pubkey - The pubkey to check
   * @returns Promise resolving to true if the pubkey exists
   */
  pubkeyExists(pubkey: string): Promise<boolean>;

  /**
   * Check if a direct follow relationship exists between two pubkeys
   * @param followerPubkey - The follower pubkey
   * @param followedPubkey - The followed pubkey
   * @returns Promise resolving to true if the relationship exists
   */
  isDirectFollow(
    followerPubkey: string,
    followedPubkey: string,
  ): Promise<boolean>;

  /**
   * Check if two pubkeys mutually follow each other
   * @param pubkey1 - First pubkey
   * @param pubkey2 - Second pubkey
   * @returns Promise resolving to true if they mutually follow each other
   */
  areMutualFollows(pubkey1: string, pubkey2: string): Promise<boolean>;

  /**
   * Get the degree (number of follows) for a pubkey
   * @param pubkey - The pubkey to check
   * @returns Promise resolving to object with outDegree (following) and inDegree (followers)
   */
  getPubkeyDegree(
    pubkey: string,
  ): Promise<{ outDegree: number; inDegree: number }>;

  /**
   * Sets the root pubkey for optimized distance calculations.
   *
   * This pre-calculates distances from the root pubkey to all other nodes
   * using a temporary table, making subsequent getShortestDistance calls
   * from this pubkey extremely fast (O(1)).
   *
   * @param pubkey - The root pubkey to optimize for
   */
  setRootPubkey(pubkey: string): Promise<void>;

  /**
   * Gets the currently configured root pubkey
   * @returns The root pubkey or null if not set
   */
  getRootPubkey(): string | null;

  /**
   * Get all users exactly at a specific distance from the root pubkey
   * @param distance - The exact distance in hops
   * @returns Promise resolving to array of pubkeys
   */
  getUsersAtDistance(distance: number): Promise<string[]>;

  /**
   * Get the distribution of users by distance from the root pubkey
   * @returns Promise resolving to a map of distance -> count
   */
  getDistanceDistribution(): Promise<Record<number, number>>;

  /**
   * Close the database connection
   * @returns Promise that resolves when the connection is closed
   */
  close(): Promise<void>;
}
