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
   * Close the database connection
   * @returns Promise that resolves when the connection is closed
   */
  close(): Promise<void>;
}
