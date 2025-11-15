/**
 * Unit tests for graph analysis functions
 */

import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { DuckDBInstance } from "@duckdb/node-api";
import {
  findShortestPath,
  isDirectFollow,
  areMutualFollows,
  getPubkeyDegree,
} from "../src/graph-analysis.js";
import { initializeDatabase, setupSchema } from "../src/database.js";
import { ingestEvents } from "../src/ingestion.js";
import {
  TEST_PUBKEYS,
  createSimpleFollowChain,
  createComplexGraph,
  createDisconnectedGraph,
  createMockKind3Event,
} from "./test-utils.js";

describe("Graph Analysis", () => {
  let instance: DuckDBInstance;
  let connection: any;

  beforeEach(async () => {
    // Create in-memory database for each test
    instance = await initializeDatabase(":memory:");
    connection = await instance.connect();
    await setupSchema(connection);
  });

  afterEach(async () => {
    if (connection) {
      connection.closeSync();
    }
  });

  describe("findShortestPath", () => {
    it("should return path with distance 0 for same pubkey", async () => {
      const path = await findShortestPath(
        connection,
        TEST_PUBKEYS.adam,
        TEST_PUBKEYS.adam,
      );

      expect(path).toEqual({
        path: [TEST_PUBKEYS.adam.toLowerCase()],
        distance: 0,
      });
    });

    it("should find direct follow path", async () => {
      const events = createSimpleFollowChain();
      await ingestEvents(connection, events);

      const path = await findShortestPath(
        connection,
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

    it("should find 2-hop path", async () => {
      const events = createSimpleFollowChain();
      await ingestEvents(connection, events);

      const path = await findShortestPath(
        connection,
        TEST_PUBKEYS.adam,
        TEST_PUBKEYS.snowden,
      );

      expect(path).toEqual({
        path: [
          TEST_PUBKEYS.adam.toLowerCase(),
          TEST_PUBKEYS.fiatjaf.toLowerCase(),
          TEST_PUBKEYS.snowden.toLowerCase(),
        ],
        distance: 2,
      });
    });

    it("should return null for disconnected nodes", async () => {
      const events = createDisconnectedGraph();
      await ingestEvents(connection, events);

      const path = await findShortestPath(
        connection,
        TEST_PUBKEYS.adam,
        TEST_PUBKEYS.bob,
      );

      expect(path).toBeNull();
    });

    it("should return null when one node doesn't exist", async () => {
      const events = createSimpleFollowChain();
      await ingestEvents(connection, events);

      const nonExistentPubkey = "a".repeat(64);
      const path = await findShortestPath(
        connection,
        TEST_PUBKEYS.adam,
        nonExistentPubkey,
      );

      expect(path).toBeNull();
    });

    it("should respect maxDepth parameter", async () => {
      const events = createSimpleFollowChain();
      await ingestEvents(connection, events);

      // With maxDepth 1, should not find 2-hop path
      const path = await findShortestPath(
        connection,
        TEST_PUBKEYS.adam,
        TEST_PUBKEYS.snowden,
        1,
      );

      expect(path).toBeNull();
    });

    it("should handle cycles in the graph", async () => {
      const events = createComplexGraph();
      await ingestEvents(connection, events);

      // adam -> fiatjaf -> snowden -> adam (cycle)
      const path = await findShortestPath(
        connection,
        TEST_PUBKEYS.adam,
        TEST_PUBKEYS.adam,
      );

      // Should return distance 0 (same node), not follow the cycle
      expect(path).toEqual({
        path: [TEST_PUBKEYS.adam.toLowerCase()],
        distance: 0,
      });
    });

    it("should normalize pubkeys to lowercase", async () => {
      const events = createSimpleFollowChain();
      await ingestEvents(connection, events);

      const path = await findShortestPath(
        connection,
        TEST_PUBKEYS.adam.toUpperCase(),
        TEST_PUBKEYS.fiatjaf.toUpperCase(),
      );

      expect(path).toEqual({
        path: [
          TEST_PUBKEYS.adam.toLowerCase(),
          TEST_PUBKEYS.fiatjaf.toLowerCase(),
        ],
        distance: 1,
      });
    });

    it("should find shortest path among multiple options", async () => {
      const events = [
        // bob -> fiatjaf -> snowden (distance 2)
        createMockKind3Event(TEST_PUBKEYS.bob, [TEST_PUBKEYS.fiatjaf], 1000),
        createMockKind3Event(
          TEST_PUBKEYS.fiatjaf,
          [TEST_PUBKEYS.snowden],
          1001,
        ),
        // alice -> sirius -> snowden (distance 2)
        createMockKind3Event(TEST_PUBKEYS.alice, [TEST_PUBKEYS.sirius], 1002),
        createMockKind3Event(TEST_PUBKEYS.sirius, [TEST_PUBKEYS.snowden], 1003),
        // adam -> snowden (distance 1 - shortest)
        createMockKind3Event(TEST_PUBKEYS.adam, [TEST_PUBKEYS.snowden], 1004),
      ];
      await ingestEvents(connection, events);

      const path = await findShortestPath(
        connection,
        TEST_PUBKEYS.adam,
        TEST_PUBKEYS.snowden,
      );

      // The shortest path should be the direct follow (distance 1)
      expect(path).not.toBeNull();
      expect(path!.distance).toBe(1);
      expect(path!.path).toHaveLength(2);
      expect(path!.path[0]).toBe(TEST_PUBKEYS.adam.toLowerCase());
      expect(path!.path[1]).toBe(TEST_PUBKEYS.snowden.toLowerCase());
    });
  });

  describe("isDirectFollow", () => {
    it("should return true for direct follow", async () => {
      const events = createSimpleFollowChain();
      await ingestEvents(connection, events);

      const result = await isDirectFollow(
        connection,
        TEST_PUBKEYS.adam,
        TEST_PUBKEYS.fiatjaf,
      );

      expect(result).toBe(true);
    });

    it("should return false for non-direct follow", async () => {
      const events = createSimpleFollowChain();
      await ingestEvents(connection, events);

      const result = await isDirectFollow(
        connection,
        TEST_PUBKEYS.adam,
        TEST_PUBKEYS.snowden,
      );

      expect(result).toBe(false);
    });

    it("should return false for non-existent relationship", async () => {
      const events = createSimpleFollowChain();
      await ingestEvents(connection, events);

      const result = await isDirectFollow(
        connection,
        TEST_PUBKEYS.adam,
        TEST_PUBKEYS.sirius,
      );

      expect(result).toBe(false);
    });

    it("should normalize pubkeys", async () => {
      const events = createSimpleFollowChain();
      await ingestEvents(connection, events);

      const result = await isDirectFollow(
        connection,
        TEST_PUBKEYS.adam.toUpperCase(),
        TEST_PUBKEYS.fiatjaf.toUpperCase(),
      );

      expect(result).toBe(true);
    });
  });

  describe("areMutualFollows", () => {
    it("should return true for mutual follows", async () => {
      const events = [
        createMockKind3Event(TEST_PUBKEYS.adam, [TEST_PUBKEYS.fiatjaf]),
        createMockKind3Event(TEST_PUBKEYS.fiatjaf, [TEST_PUBKEYS.adam]),
      ];
      await ingestEvents(connection, events);

      const result = await areMutualFollows(
        connection,
        TEST_PUBKEYS.adam,
        TEST_PUBKEYS.fiatjaf,
      );

      expect(result).toBe(true);
    });

    it("should return false for one-way follow", async () => {
      const events = createSimpleFollowChain();
      await ingestEvents(connection, events);

      const result = await areMutualFollows(
        connection,
        TEST_PUBKEYS.adam,
        TEST_PUBKEYS.fiatjaf,
      );

      expect(result).toBe(false);
    });

    it("should return false for no relationship", async () => {
      const events = createSimpleFollowChain();
      await ingestEvents(connection, events);

      const result = await areMutualFollows(
        connection,
        TEST_PUBKEYS.adam,
        TEST_PUBKEYS.sirius,
      );

      expect(result).toBe(false);
    });

    it("should normalize pubkeys", async () => {
      const events = [
        createMockKind3Event(TEST_PUBKEYS.adam, [TEST_PUBKEYS.fiatjaf]),
        createMockKind3Event(TEST_PUBKEYS.fiatjaf, [TEST_PUBKEYS.adam]),
      ];
      await ingestEvents(connection, events);

      const result = await areMutualFollows(
        connection,
        TEST_PUBKEYS.adam.toUpperCase(),
        TEST_PUBKEYS.fiatjaf.toUpperCase(),
      );

      expect(result).toBe(true);
    });
  });

  describe("getPubkeyDegree", () => {
    it("should return zero degrees for non-existent pubkey", async () => {
      const degree = await getPubkeyDegree(connection, TEST_PUBKEYS.adam);

      expect(degree).toEqual({
        outDegree: 0,
        inDegree: 0,
      });
    });

    it("should return correct degrees for pubkey with follows", async () => {
      const events = [
        createMockKind3Event(TEST_PUBKEYS.adam, [
          TEST_PUBKEYS.fiatjaf,
          TEST_PUBKEYS.snowden,
        ]),
        createMockKind3Event(TEST_PUBKEYS.sirius, [TEST_PUBKEYS.adam]),
      ];
      await ingestEvents(connection, events);

      const degree = await getPubkeyDegree(connection, TEST_PUBKEYS.adam);

      expect(degree).toEqual({
        outDegree: 2, // Follows fiatjaf and snowden
        inDegree: 1, // Followed by sirius
      });
    });

    it("should handle pubkey with only followers", async () => {
      const events = [
        createMockKind3Event(TEST_PUBKEYS.fiatjaf, [TEST_PUBKEYS.adam]),
        createMockKind3Event(TEST_PUBKEYS.snowden, [TEST_PUBKEYS.adam]),
      ];
      await ingestEvents(connection, events);

      const degree = await getPubkeyDegree(connection, TEST_PUBKEYS.adam);

      expect(degree).toEqual({
        outDegree: 0,
        inDegree: 2, // Followed by fiatjaf and snowden
      });
    });

    it("should normalize pubkey", async () => {
      const events = [
        createMockKind3Event(TEST_PUBKEYS.adam, [TEST_PUBKEYS.fiatjaf]),
      ];
      await ingestEvents(connection, events);

      const degree = await getPubkeyDegree(
        connection,
        TEST_PUBKEYS.adam.toUpperCase(),
      );

      expect(degree).toEqual({
        outDegree: 1,
        inDegree: 0,
      });
    });
  });
});
