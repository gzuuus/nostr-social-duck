/**
 * Parser for Nostr Kind 3 (follow list) events
 */

import type {
  NostrEvent,
  FollowRelationship,
  ParsedKind3Event,
} from "./types.js";
import { isHex, isHexKey } from "./utils.js";

/**
 * Validates that an event is a Kind 3 (follow list) event
 * @param event - The Nostr event to validate
 * @throws Error if the event is not a valid Kind 3 event
 */
export function validateKind3Event(event: NostrEvent): void {
  if (!event) {
    throw new Error("Event is required");
  }

  if (event.kind !== 3) {
    throw new Error(`Expected Kind 3 event, got Kind ${event.kind}`);
  }

  if (typeof event.created_at !== "number" || event.created_at < 0) {
    throw new Error("Invalid created_at: must be a positive number");
  }

  if (!Array.isArray(event.tags)) {
    throw new Error("Invalid tags: must be an array");
  }
}

/**
 * Parses a Kind 3 Nostr event and extracts follow relationships
 *
 * According to NIP-02, Kind 3 events contain a list of 'p' tags,
 * where each tag represents a followed profile:
 * ["p", <32-bytes hex key>, <relay URL>, <petname>]
 *
 * @param event - The Nostr Kind 3 event to parse
 * @param skipValidation - Whether to skip validation (default: false)
 * @returns Parsed event data with follow relationships
 * @throws Error if the event is invalid
 */
export function parseKind3Event(
  event: NostrEvent,
  skipValidation: boolean = false,
): ParsedKind3Event {
  // Validate the event first unless skipped
  if (!skipValidation) {
    validateKind3Event(event);
  }

  // Extract follow relationships from 'p' tags
  const follows: FollowRelationship[] = [];

  // Optimized loop with minimal operations
  for (let i = 0; i < event.tags.length; i++) {
    const tag = event.tags[i];

    // Must be an array with at least 2 elements
    if (!Array.isArray(tag) || tag.length < 2) {
      continue;
    }

    // First element must be 'p'
    if (tag[0] !== "p") {
      continue;
    }

    // Second element (pubkey) must be a valid 64-character hex string
    const followedPubkey = tag[1];
    if (!isHexKey(followedPubkey)) {
      continue;
    }

    // Add follow relationship
    follows.push({
      follower_pubkey: event.pubkey,
      followed_pubkey: followedPubkey.toLowerCase(), // Normalize to lowercase
      created_at: event.created_at,
    });
  }

  return {
    follows,
    event,
  };
}

/**
 * Normalizes a pubkey to lowercase
 * @param pubkey - The pubkey to normalize
 * @returns Normalized pubkey
 * @throws Error if pubkey is invalid
 */
export function normalizePubkey(pubkey: string): string {
  if (!isHexKey(pubkey)) {
    throw new Error(`Invalid pubkey format: ${pubkey}`);
  }
  return pubkey.toLowerCase();
}
