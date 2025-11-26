/**
 * Tests for edge cases and error handling
 */

import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { DuckDBSocialGraphAnalyzer } from "../src/analyzer.js";
import type { NostrEvent } from "../src/types.js";
import { TEST_PUBKEYS, createMockKind3Event } from "./test-utils.js";

describe("Edge Cases and Error Handling", () => {
  let analyzer: DuckDBSocialGraphAnalyzer;

  beforeEach(async () => {
    analyzer = await DuckDBSocialGraphAnalyzer.create();
  });

  afterEach(async () => {
    if (analyzer && !analyzer.isClosed()) {
      await analyzer.close();
    }
  });

  describe("Large Follow Lists", () => {
    it("should handle duplicate follows in same event", async () => {
      const event = createMockKind3Event(
        TEST_PUBKEYS.adam,
        [TEST_PUBKEYS.fiatjaf, TEST_PUBKEYS.fiatjaf, TEST_PUBKEYS.snowden], // Duplicate
        1234567890,
      );

      await analyzer.ingestEvent(event);

      const stats = await analyzer.getStats();
      // Should deduplicate within the same event
      expect(stats.totalFollows).toBe(2);
    });
  });

  describe("Event Timestamp Handling", () => {
    it("should handle events with same timestamp", async () => {
      const timestamp = 1234567890;

      const event1 = createMockKind3Event(
        TEST_PUBKEYS.adam,
        [TEST_PUBKEYS.fiatjaf],
        timestamp,
      );

      const event2 = createMockKind3Event(
        TEST_PUBKEYS.adam,
        [TEST_PUBKEYS.snowden],
        timestamp, // Same timestamp
      );

      // When timestamps are equal, the behavior might be implementation-dependent
      // In our implementation, we keep the first one encountered
      await analyzer.ingestEvents([event1, event2]);

      const stats = await analyzer.getStats();
      expect(stats.totalFollows).toBe(1);
    });

    it("should handle very old and very new timestamps", async () => {
      const ancientEvent = createMockKind3Event(
        TEST_PUBKEYS.adam,
        [TEST_PUBKEYS.fiatjaf],
        1, // Very old
      );

      const futureEvent = createMockKind3Event(
        TEST_PUBKEYS.adam,
        [TEST_PUBKEYS.snowden],
        4102444800, // Year 2100
      );

      await analyzer.ingestEvents([ancientEvent, futureEvent]);

      // Should keep the future event (latest timestamp)
      const stats = await analyzer.getStats();
      expect(stats.totalFollows).toBe(1);

      const path = await analyzer.getShortestPath(
        TEST_PUBKEYS.adam,
        TEST_PUBKEYS.snowden,
      );
      expect(path).not.toBeNull();
    });
  });

  describe("Pubkey Format Edge Cases", () => {
    it("should handle pubkeys with mixed case", async () => {
      const mixedCasePubkey = TEST_PUBKEYS.fiatjaf.toUpperCase();

      const event = createMockKind3Event(
        TEST_PUBKEYS.adam,
        [mixedCasePubkey],
        1234567890,
      );

      await analyzer.ingestEvent(event);

      // Should normalize to lowercase
      const path = await analyzer.getShortestPath(
        TEST_PUBKEYS.adam,
        TEST_PUBKEYS.fiatjaf.toLowerCase(),
      );
      expect(path).not.toBeNull();
    });
  });

  describe("Tag Format Edge Cases", () => {
    it("should handle tags with extra elements", async () => {
      const event = createMockKind3Event(
        TEST_PUBKEYS.adam,
        [TEST_PUBKEYS.fiatjaf, TEST_PUBKEYS.snowden],
        1234567890,
      );
      // Manually add extra tag elements for testing
      event.tags[0] = [
        "p",
        TEST_PUBKEYS.fiatjaf,
        "wss://relay.example.com",
        "Fiatjaf",
      ];

      await analyzer.ingestEvent(event);

      const stats = await analyzer.getStats();
      expect(stats.totalFollows).toBe(2);
    });

    it("should ignore malformed p tags", async () => {
      const event = createMockKind3Event(
        TEST_PUBKEYS.adam,
        [TEST_PUBKEYS.fiatjaf],
        1234567890,
      );
      // Manually add malformed tags for testing
      event.tags = [
        ["p"], // Missing pubkey
        ["p", "invalid-pubkey"], // Invalid pubkey
        ["p", TEST_PUBKEYS.fiatjaf], // Valid
        ["e", "some-event"], // Wrong tag type
      ];

      await analyzer.ingestEvent(event);

      const stats = await analyzer.getStats();
      expect(stats.totalFollows).toBe(1); // Only the valid one
    });

    it("should handle empty tags array", async () => {
      const event = createMockKind3Event(TEST_PUBKEYS.adam, [], 1234567890);

      await analyzer.ingestEvent(event);

      const stats = await analyzer.getStats();
      expect(stats.totalFollows).toBe(0);
    });
  });

  describe("Graph Structure Edge Cases", () => {
    it("should handle self-follows", async () => {
      const event = createMockKind3Event(
        TEST_PUBKEYS.adam,
        [TEST_PUBKEYS.adam], // Self-follow
        1234567890,
      );

      await analyzer.ingestEvent(event);

      const path = await analyzer.getShortestPath(
        TEST_PUBKEYS.adam,
        TEST_PUBKEYS.adam,
      );
      expect(path).toEqual({
        path: [TEST_PUBKEYS.adam.toLowerCase()],
        distance: 0,
      });
    });

    it("should handle cycles in the graph", async () => {
      const events: NostrEvent[] = [
        createMockKind3Event(TEST_PUBKEYS.adam, [TEST_PUBKEYS.fiatjaf], 1000),
        createMockKind3Event(TEST_PUBKEYS.fiatjaf, [TEST_PUBKEYS.adam], 1001), // Creates cycle
      ];

      await analyzer.ingestEvents(events);

      // Should not get stuck in infinite loop
      const path = await analyzer.getShortestPath(
        TEST_PUBKEYS.adam,
        TEST_PUBKEYS.fiatjaf,
      );
      expect(path).toEqual({
        path: [
          TEST_PUBKEYS.adam.toLowerCase(),
          TEST_PUBKEYS.fiatjaf.toLowerCase(),
        ],
        distance: 1,
      });
    });

    it("should handle disconnected components", async () => {
      const events: NostrEvent[] = [
        createMockKind3Event(TEST_PUBKEYS.adam, [TEST_PUBKEYS.fiatjaf], 1000),
        createMockKind3Event(TEST_PUBKEYS.bob, [TEST_PUBKEYS.alice], 1001),
      ];

      await analyzer.ingestEvents(events);

      // Should not find path between disconnected components
      const path = await analyzer.getShortestPath(
        TEST_PUBKEYS.adam,
        TEST_PUBKEYS.bob,
      );
      expect(path).toBeNull();
    });
  });
});
