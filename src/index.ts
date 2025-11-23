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
