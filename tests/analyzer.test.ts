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
  createComplexGraph,
  createDisconnectedGraph,
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

  describe("getShortestDistancesBatch", () => {
    it("should calculate distances to multiple targets in batch", async () => {
      const events = createComplexGraph();
      await analyzer.ingestEvents(events);

      const distances = await analyzer.getShortestDistancesBatch(
        TEST_PUBKEYS.adam,
        [TEST_PUBKEYS.fiatjaf, TEST_PUBKEYS.snowden, TEST_PUBKEYS.sirius],
      );

      expect(distances.size).toBe(3);
      expect(distances.get(TEST_PUBKEYS.fiatjaf)).toBe(1); // Direct follow
      expect(distances.get(TEST_PUBKEYS.snowden)).toBe(1); // Direct follow
      expect(distances.get(TEST_PUBKEYS.sirius)).toBe(2); // Through fiatjaf
    });

    it("should handle disconnected nodes correctly", async () => {
      const events = createDisconnectedGraph();
      await analyzer.ingestEvents(events);

      const distances = await analyzer.getShortestDistancesBatch(
        TEST_PUBKEYS.adam,
        [TEST_PUBKEYS.fiatjaf, TEST_PUBKEYS.bob, TEST_PUBKEYS.alice],
      );

      expect(distances.size).toBe(3);
      expect(distances.get(TEST_PUBKEYS.fiatjaf)).toBe(1); // Direct follow
      expect(distances.get(TEST_PUBKEYS.bob)).toBeNull(); // No path
      expect(distances.get(TEST_PUBKEYS.alice)).toBeNull(); // No path
    });

    it("should handle empty target array", async () => {
      const events = createSimpleFollowChain();
      await analyzer.ingestEvents(events);

      const distances = await analyzer.getShortestDistancesBatch(
        TEST_PUBKEYS.adam,
        [],
      );

      expect(distances.size).toBe(0);
    });

    it("should handle duplicate target pubkeys", async () => {
      const events = createSimpleFollowChain();
      await analyzer.ingestEvents(events);

      const distances = await analyzer.getShortestDistancesBatch(
        TEST_PUBKEYS.adam,
        [TEST_PUBKEYS.fiatjaf, TEST_PUBKEYS.fiatjaf, TEST_PUBKEYS.snowden],
      );

      expect(distances.size).toBe(2); // Duplicates should be removed
      expect(distances.get(TEST_PUBKEYS.fiatjaf)).toBe(1);
      expect(distances.get(TEST_PUBKEYS.snowden)).toBe(2);
    });

    it("should work with root pubkey optimization", async () => {
      const events = createComplexGraph();
      await analyzer.ingestEvents(events);

      // Set root pubkey to adam
      await analyzer.setRootPubkey(TEST_PUBKEYS.adam);

      const distances = await analyzer.getShortestDistancesBatch(
        TEST_PUBKEYS.adam,
        [TEST_PUBKEYS.fiatjaf, TEST_PUBKEYS.snowden, TEST_PUBKEYS.sirius],
      );

      expect(distances.size).toBe(3);
      expect(distances.get(TEST_PUBKEYS.fiatjaf)).toBe(1);
      expect(distances.get(TEST_PUBKEYS.snowden)).toBe(1);
      expect(distances.get(TEST_PUBKEYS.sirius)).toBe(2);
    });

    it("should handle non-root pubkey with root optimization available", async () => {
      const events = createComplexGraph();
      await analyzer.ingestEvents(events);

      // Set root pubkey to adam
      await analyzer.setRootPubkey(TEST_PUBKEYS.adam);

      // Query from fiatjaf (not the root)
      const distances = await analyzer.getShortestDistancesBatch(
        TEST_PUBKEYS.fiatjaf,
        [TEST_PUBKEYS.adam, TEST_PUBKEYS.snowden, TEST_PUBKEYS.sirius],
      );

      expect(distances.size).toBe(3);
      expect(distances.get(TEST_PUBKEYS.adam)).toBe(2); // Through snowden or sirius (fiatjaf -> snowden -> adam)
      expect(distances.get(TEST_PUBKEYS.snowden)).toBe(1); // Direct follow
      expect(distances.get(TEST_PUBKEYS.sirius)).toBe(1); // Direct follow
    });

    it("should throw error when analyzer is closed", async () => {
      await analyzer.close();

      await expect(
        analyzer.getShortestDistancesBatch(TEST_PUBKEYS.adam, [
          TEST_PUBKEYS.fiatjaf,
        ]),
      ).rejects.toThrow("Analyzer has been closed");
    });
  });
});

describe("persistent database", () => {
  const tmpDir = "./tmp";
  const testDbPath = `${tmpDir}/test-persistent.db`;

  beforeEach(async () => {
    // Create tmp directory if it doesn't exist
    await Bun.$`mkdir -p ${tmpDir}`;
  });

  afterEach(async () => {
    // Clean up entire tmp directory after each test
    try {
      await Bun.$`rm -rf ${tmpDir}`;
    } catch (error) {
      // Ignore errors if directory doesn't exist
    }
  });

  it("should create and use persistent database", async () => {
    // Create analyzer with persistent database
    const persistentAnalyzer = await DuckDBSocialGraphAnalyzer.create({
      dbPath: testDbPath,
      maxDepth: 6,
    });

    try {
      // Verify the analyzer was created successfully
      expect(persistentAnalyzer).toBeInstanceOf(DuckDBSocialGraphAnalyzer);
      expect(persistentAnalyzer.getMaxDepth()).toBe(6);
      expect(persistentAnalyzer.isClosed()).toBe(false);

      // Ingest some test data
      const events = createSimpleFollowChain();
      await persistentAnalyzer.ingestEvents(events);

      // Verify data was ingested
      const stats = await persistentAnalyzer.getStats();
      expect(stats.totalFollows).toBe(2);
      expect(stats.uniqueFollowers).toBe(2);

      // Test path finding
      const path = await persistentAnalyzer.getShortestPath(
        TEST_PUBKEYS.adam,
        TEST_PUBKEYS.snowden,
      );
      expect(path).not.toBeNull();
      expect(path?.distance).toBe(2);
      expect(path?.path).toHaveLength(3);
      expect(path?.path[0]).toBe(TEST_PUBKEYS.adam);
      expect(path?.path[1]).toBe(TEST_PUBKEYS.fiatjaf);
      expect(path?.path[2]).toBe(TEST_PUBKEYS.snowden);

      // Close the analyzer
      await persistentAnalyzer.close();
      expect(persistentAnalyzer.isClosed()).toBe(true);

      // Verify database file exists
      const fileExists = await Bun.file(testDbPath).exists();
      expect(fileExists).toBe(true);

      // Re-open the database and verify data persists
      const reopenedAnalyzer = await DuckDBSocialGraphAnalyzer.create({
        dbPath: testDbPath,
        maxDepth: 6,
      });

      try {
        // Verify the data is still there
        const reopenedStats = await reopenedAnalyzer.getStats();
        expect(reopenedStats.totalFollows).toBe(2);
        expect(reopenedStats.uniqueFollowers).toBe(2);

        // Verify path finding still works
        const reopenedPath = await reopenedAnalyzer.getShortestPath(
          TEST_PUBKEYS.adam,
          TEST_PUBKEYS.snowden,
        );
        expect(reopenedPath).not.toBeNull();
        expect(reopenedPath?.distance).toBe(2);
      } finally {
        await reopenedAnalyzer.close();
      }
    } finally {
      if (!persistentAnalyzer.isClosed()) {
        await persistentAnalyzer.close();
      }
    }
  });

  it("should handle multiple connections to same persistent database", async () => {
    // Create first analyzer
    const analyzer1 = await DuckDBSocialGraphAnalyzer.create({
      dbPath: testDbPath,
      maxDepth: 6,
    });

    try {
      // Ingest data with first analyzer
      const events = createSimpleFollowChain();
      await analyzer1.ingestEvents(events);

      // Create second analyzer pointing to same database
      const analyzer2 = await DuckDBSocialGraphAnalyzer.create({
        dbPath: testDbPath,
        maxDepth: 6,
      });

      try {
        // Both analyzers should see the same data
        const stats1 = await analyzer1.getStats();
        const stats2 = await analyzer2.getStats();

        expect(stats1.totalFollows).toBe(2);
        expect(stats2.totalFollows).toBe(2);
        expect(stats1.uniqueFollowers).toBe(2);
        expect(stats2.uniqueFollowers).toBe(2);

        // Both should be able to find the same path
        const path1 = await analyzer1.getShortestPath(
          TEST_PUBKEYS.adam,
          TEST_PUBKEYS.snowden,
        );
        const path2 = await analyzer2.getShortestPath(
          TEST_PUBKEYS.adam,
          TEST_PUBKEYS.snowden,
        );

        expect(path1).not.toBeNull();
        expect(path2).not.toBeNull();
        expect(path1?.distance).toBe(2);
        expect(path2?.distance).toBe(2);
      } finally {
        await analyzer2.close();
      }
    } finally {
      await analyzer1.close();
    }
  });
});
