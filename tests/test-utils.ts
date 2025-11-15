/**
 * Test utilities for Nostr Social Duck library tests
 */

import type { NostrEvent } from "../src/types.js";

/**
 * Test pubkeys for creating consistent test data
 */
export const TEST_PUBKEYS = {
  adam: "020f2d21ae09bf35fcdfb65decf1478b846f5f728ab30c5eaabcd6d081a81c3e",
  fiatjaf: "3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d",
  snowden: "84dee6e676e5bb67b4ad4e042cf70cbd8681155db535942fcc6a0533858a7240",
  sirius: "4523be58d395b1b196a9b8c82b038b6895cb02b683d0c253a955068dba1facd0",
  bob: "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
  alice: "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321",
} as const;

/**
 * Generates a valid SHA256 hex string for event IDs
 */
function generateValidHash(): string {
  // Create a deterministic but valid-looking SHA256 hash
  let hash = "";
  for (let i = 0; i < 64; i++) {
    hash += Math.floor(Math.random() * 16).toString(16);
  }
  return hash;
}

/**
 * Creates a mock Kind 3 Nostr event with valid SHA256 event IDs
 */
export function createMockKind3Event(
  pubkey: string,
  followedPubkeys: string[],
  timestamp: number = Math.floor(Date.now() / 1000),
): NostrEvent {
  const eventId = generateValidHash();

  return {
    id: eventId,
    pubkey,
    created_at: timestamp,
    kind: 3,
    tags: followedPubkeys.map((followed) => ["p", followed]),
    content: "",
    sig: generateValidHash(),
  };
}

/**
 * Creates a simple follow chain for testing
 * adam -> fiatjaf -> snowden
 */
export function createSimpleFollowChain(): NostrEvent[] {
  const now = Math.floor(Date.now() / 1000);

  return [
    createMockKind3Event(TEST_PUBKEYS.adam, [TEST_PUBKEYS.fiatjaf], now),
    createMockKind3Event(TEST_PUBKEYS.fiatjaf, [TEST_PUBKEYS.snowden], now + 1),
  ];
}

/**
 * Creates a more complex graph for testing
 * adam -> fiatjaf, snowden
 * fiatjaf -> snowden, sirius
 * snowden -> adam
 * sirius -> adam
 */
export function createComplexGraph(): NostrEvent[] {
  const now = Math.floor(Date.now() / 1000);

  return [
    createMockKind3Event(
      TEST_PUBKEYS.adam,
      [TEST_PUBKEYS.fiatjaf, TEST_PUBKEYS.snowden],
      now,
    ),
    createMockKind3Event(
      TEST_PUBKEYS.fiatjaf,
      [TEST_PUBKEYS.snowden, TEST_PUBKEYS.sirius],
      now + 1,
    ),
    createMockKind3Event(TEST_PUBKEYS.snowden, [TEST_PUBKEYS.adam], now + 2),
    createMockKind3Event(TEST_PUBKEYS.sirius, [TEST_PUBKEYS.adam], now + 3),
  ];
}

/**
 * Creates a disconnected graph for testing
 * adam -> fiatjaf
 * bob -> alice
 */
export function createDisconnectedGraph(): NostrEvent[] {
  const now = Math.floor(Date.now() / 1000);

  return [
    createMockKind3Event(TEST_PUBKEYS.adam, [TEST_PUBKEYS.fiatjaf], now),
    createMockKind3Event(TEST_PUBKEYS.bob, [TEST_PUBKEYS.alice], now + 1),
  ];
}

/**
 * Helper to measure execution time of async function
 */
export async function measureTime<T>(
  fn: () => Promise<T>,
): Promise<{ result: T; time: number }> {
  const start = performance.now();
  const result = await fn();
  const end = performance.now();
  return { result, time: end - start };
}
