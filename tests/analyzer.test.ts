/**
 * Integration tests for DuckDBSocialGraphAnalyzer class
 */

import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { DuckDBSocialGraphAnalyzer } from "../src/analyzer.js";
import type { NostrEvent } from "../src/types.js";
import {
  TEST_PUBKEYS,
  createSimpleFollowChain,
  createMockKind3Event,
} from "./test-utils.js";

describe("DuckDBSocialGraphAnalyzer", () => {
  let analyzer: DuckDBSocialGraphAnalyzer;

  beforeEach(async () => {
    analyzer = await DuckDBSocialGraphAnalyzer.create({
      maxDepth: 6,
    });
  });

  afterEach(async () => {
    if (analyzer && !analyzer.isClosed()) {
      await analyzer.close();
    }
  });

  describe("create", () => {
    it("should create analyzer with default config", async () => {
      const defaultAnalyzer = await DuckDBSocialGraphAnalyzer.create();

      expect(defaultAnalyzer).toBeInstanceOf(DuckDBSocialGraphAnalyzer);
      expect(defaultAnalyzer.getMaxDepth()).toBe(6);
      expect(defaultAnalyzer.isClosed()).toBe(false);

      await defaultAnalyzer.close();
    });

    describe("ingestEvent", () => {
      it("should ingest a single event", async () => {
        const events = createSimpleFollowChain();
        await analyzer.ingestEvent(events[0]!);

        const stats = await analyzer.getStats();
        expect(stats.totalFollows).toBe(1);
        expect(stats.uniqueFollowers).toBe(1);
      });

      it("should handle event replacement (latest event wins)", async () => {
        const event1 = createMockKind3Event(
          TEST_PUBKEYS.adam,
          [TEST_PUBKEYS.fiatjaf],
          1000,
        );

        const event2 = createMockKind3Event(
          TEST_PUBKEYS.adam,
          [TEST_PUBKEYS.snowden], // Different follow
          2000, // Later timestamp
        );

        await analyzer.ingestEvent(event1);
        await analyzer.ingestEvent(event2);

        const stats = await analyzer.getStats();
        // Should only have the latest event's follows
        expect(stats.totalFollows).toBe(1);
        expect(stats.uniqueEvents).toBe(1);

        // Should follow snowden, not fiatjaf
        const path = await analyzer.getShortestPath(
          TEST_PUBKEYS.adam,
          TEST_PUBKEYS.snowden,
        );
        expect(path).not.toBeNull();

        const oldPath = await analyzer.getShortestPath(
          TEST_PUBKEYS.adam,
          TEST_PUBKEYS.fiatjaf,
        );
        expect(oldPath).toBeNull();
      });

      it("should throw error for invalid event", async () => {
        const invalidEvent = {
          id: "invalid",
          pubkey: "not-a-pubkey",
          created_at: 1234567890,
          kind: 3,
          tags: [],
          content: "",
          sig: "sig",
        } as NostrEvent;

        await expect(analyzer.ingestEvent(invalidEvent)).rejects.toThrow();
      });
    });

    describe("error handling", () => {
      it("should handle invalid pubkeys in getShortestPath", async () => {
        const invalidPubkey = "not-a-valid-pubkey";

        await expect(
          analyzer.getShortestPath(TEST_PUBKEYS.adam, invalidPubkey),
        ).rejects.toThrow();
      });

      it("should handle non-existent pubkeys gracefully", async () => {
        const nonExistentPubkey = "a".repeat(64); // Valid format but not in graph

        const path = await analyzer.getShortestPath(
          TEST_PUBKEYS.adam,
          nonExistentPubkey,
        );

        expect(path).toBeNull();
      });
    });
  });
});
