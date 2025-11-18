/**
 * Nostr Social Graph Analysis Library
 *
 * A TypeScript library for performing social graph analysis on Nostr Kind 3 events
 * (follow lists) using DuckDB for efficient graph traversal.
 *
 * @packageDocumentation
 */

// Main analyzer class
export { DuckDBSocialGraphAnalyzer } from "./analyzer.js";

// Type definitions
export type {
  NostrEvent,
  FollowRelationship,
  ParsedKind3Event,
  SocialPath,
  SocialGraphConfig,
  GraphStats,
  SocialGraphAnalyzer,
} from "./types.js";

// Parser utilities
export {
  parseKind3Event,
  validateKind3Event,
  normalizePubkey,
} from "./parser.js";

// Database utilities (for advanced usage)
export {
  initializeDatabase,
  setupSchema,
  getTableStats,
  pubkeyExists,
  getFollowing,
  getFollowers,
} from "./database.js";

// Graph analysis utilities (for advanced usage)
export {
  findShortestPath,
  findShortestDistance,
  isDirectFollow,
  areMutualFollows,
  getPubkeyDegree,
  getUsersWithinDistance,
  getAllUniquePubkeys,
} from "./graph-analysis.js";

// Ingestion utilities (for advanced usage)
export {
  ingestEvent,
  ingestEvents,
  deleteFollowsForPubkey,
} from "./ingestion.js";
