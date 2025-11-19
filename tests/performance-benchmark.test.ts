/**
 * Performance benchmark tests for graph analysis functions
 *
 * These tests focus on measuring the performance improvements from our optimizations,
 * particularly the new getShortestDistance method vs getShortestPath.
 *
 * Uses the existing database with real data for realistic performance measurements.
 */

import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { DuckDBInstance } from "@duckdb/node-api";
import {
  findShortestPath,
  findShortestDistance,
  getUsersWithinDistance,
} from "../src/graph-analysis.js";
import { initializeDatabase, setupSchema } from "../src/database.js";
import { DuckDBSocialGraphAnalyzer } from "../src/analyzer.js";

describe("Performance Benchmarks", () => {
  let instance: DuckDBInstance;
  let connection: any;

  beforeEach(async () => {
    // Use the existing database with real data
    instance = await initializeDatabase("data/social-graph.db");
    connection = await instance.connect();
    await setupSchema(connection);
  });

  afterEach(async () => {
    if (connection) {
      connection.closeSync();
    }
  });

  describe("getShortestPath vs getShortestDistance", () => {
    it("should show performance difference for multi-hop paths", async () => {
      // Use a simple test case that we know works
      const fromPubkey =
        "6e468422dfb74a5738702a8823b9b28168abab8655faacb6853cd0ee15deee93";
      const toPubkey =
        "ee07e263a68afd3c9cc25bec9bde31c25b4156e71466bb3df9185be11be01122";

      // Test multiple queries to get reliable timing
      const iterations = 5;
      let pathTotalTime = 0;
      let distanceTotalTime = 0;
      let distance = null;
      for (let i = 0; i < iterations; i++) {
        // Measure getShortestPath
        const pathStart = performance.now();
        const pathResult = await findShortestPath(
          connection,
          fromPubkey,
          toPubkey,
          3,
        );
        const pathEnd = performance.now();
        pathTotalTime += pathEnd - pathStart;

        // Measure getShortestDistance
        const distanceStart = performance.now();
        const distanceResult = await findShortestDistance(
          connection,
          fromPubkey,
          toPubkey,
          3,
        );
        distance = distanceResult;
        const distanceEnd = performance.now();
        distanceTotalTime += distanceEnd - distanceStart;

        // Verify both methods return consistent results
        if (pathResult && distanceResult !== null) {
          expect(pathResult.distance).toBe(distanceResult);
        } else {
          expect(pathResult).toBeNull();
          expect(distanceResult).toBeNull();
        }
      }

      const pathAvgTime = pathTotalTime / iterations;
      const distanceAvgTime = distanceTotalTime / iterations;

      console.log(
        `Multi-hop path (${distance} distance) - getShortestPath: ${pathAvgTime.toFixed(2)}ms`,
      );
      console.log(
        `Multi-hop path (${distance} distance) - getShortestDistance: ${distanceAvgTime.toFixed(2)}ms`,
      );

      if (distanceAvgTime > 0) {
        console.log(
          `Performance ratio: ${(pathAvgTime / distanceAvgTime).toFixed(2)}x`,
        );
      }
    }, 20000); // 10 second timeout

    it("should handle direct connections efficiently", async () => {
      // Find some direct connections in the graph
      const nearbyUsers = await getUsersWithinDistance(
        connection,
        "6e468422dfb74a5738702a8823b9b28168abab8655faacb6853cd0ee15deee93",
        1,
      );

      if (!nearbyUsers || nearbyUsers.length === 0) {
        console.log("No direct connections found for test");
        return;
      }

      const directConnectionPubkey = nearbyUsers[0];

      const iterations = 10;
      let pathTotalTime = 0;
      let distanceTotalTime = 0;

      for (let i = 0; i < iterations; i++) {
        // Direct connection (distance 1)
        const pathStart = performance.now();
        const pathResult = await findShortestPath(
          connection,
          "6e468422dfb74a5738702a8823b9b28168abab8655faacb6853cd0ee15deee93",
          directConnectionPubkey,
          3,
        );
        const pathEnd = performance.now();
        pathTotalTime += pathEnd - pathStart;

        const distanceStart = performance.now();
        const distanceResult = await findShortestDistance(
          connection,
          "6e468422dfb74a5738702a8823b9b28168abab8655faacb6853cd0ee15deee93",
          directConnectionPubkey,
          3,
        );
        const distanceEnd = performance.now();
        distanceTotalTime += distanceEnd - distanceStart;

        expect(pathResult?.distance).toBe(1);
        expect(distanceResult).toBe(1);
      }

      const pathAvgTime = pathTotalTime / iterations;
      const distanceAvgTime = distanceTotalTime / iterations;

      console.log(
        `Direct connection - getShortestPath: ${pathAvgTime.toFixed(2)}ms`,
      );
      console.log(
        `Direct connection - getShortestDistance: ${distanceAvgTime.toFixed(2)}ms`,
      );

      // Both should be very fast for direct connections
      expect(pathAvgTime).toBeLessThan(15);
      expect(distanceAvgTime).toBeLessThan(15);
    });

    it("should handle same node queries efficiently", async () => {
      const iterations = 20;
      let pathTotalTime = 0;
      let distanceTotalTime = 0;

      for (let i = 0; i < iterations; i++) {
        // Same node (distance 0)
        const pathStart = performance.now();
        const pathResult = await findShortestPath(
          connection,
          "6e468422dfb74a5738702a8823b9b28168abab8655faacb6853cd0ee15deee93",
          "6e468422dfb74a5738702a8823b9b28168abab8655faacb6853cd0ee15deee93",
          3,
        );
        const pathEnd = performance.now();
        pathTotalTime += pathEnd - pathStart;

        const distanceStart = performance.now();
        const distanceResult = await findShortestDistance(
          connection,
          "6e468422dfb74a5738702a8823b9b28168abab8655faacb6853cd0ee15deee93",
          "6e468422dfb74a5738702a8823b9b28168abab8655faacb6853cd0ee15deee93",
          3,
        );
        const distanceEnd = performance.now();
        distanceTotalTime += distanceEnd - distanceStart;

        expect(pathResult?.distance).toBe(0);
        expect(distanceResult).toBe(0);
      }

      const pathAvgTime = pathTotalTime / iterations;
      const distanceAvgTime = distanceTotalTime / iterations;

      console.log(`Same node - getShortestPath: ${pathAvgTime.toFixed(2)}ms`);
      console.log(
        `Same node - getShortestDistance: ${distanceAvgTime.toFixed(2)}ms`,
      );

      // Both should be extremely fast for same node
      expect(pathAvgTime).toBeLessThan(5);
      expect(distanceAvgTime).toBeLessThan(5);
    });
  });

  describe("rootPubkey optimization", () => {
    it("should show dramatic performance improvement with setRootPubkey", async () => {
      // Create analyzer with real data
      const analyzer = await DuckDBSocialGraphAnalyzer.create({
        dbPath: "data/social-graph.db",
        maxDepth: 6,
      });

      try {
        // Find some test pubkeys to use
        const allPubkeys = await analyzer.getAllUniquePubkeys();
        if (allPubkeys.length < 25) {
          console.log("Not enough test data for performance comparison");
          return;
        }

        const rootPubkey = allPubkeys[0];
        const targetPubkeys = allPubkeys.slice(1, 24); // Test 5 different targets

        console.log(
          `Testing rootPubkey optimization with graph containing ${allPubkeys.length} pubkeys`,
        );

        // Test WITHOUT rootPubkey optimization
        const baselineTimes: number[] = [];
        for (const targetPubkey of targetPubkeys) {
          const startTime = performance.now();
          const distance = await analyzer.getShortestDistance(
            rootPubkey,
            targetPubkey,
          );
          const endTime = performance.now();
          baselineTimes.push(endTime - startTime);
          console.log(
            `Baseline: ${rootPubkey.substring(0, 8)}... -> ${targetPubkey.substring(0, 8)}...: ${distance} hops, ${(endTime - startTime).toFixed(2)}ms`,
          );
        }

        // Set rootPubkey for optimization
        const setRootStart = performance.now();
        await analyzer.setRootPubkey(rootPubkey);
        const setRootEnd = performance.now();
        console.log(
          `setRootPubkey took ${(setRootEnd - setRootStart).toFixed(2)}ms (one-time cost)`,
        );

        // Test WITH rootPubkey optimization
        const optimizedTimes: number[] = [];
        for (const targetPubkey of targetPubkeys) {
          const startTime = performance.now();
          const distance = await analyzer.getShortestDistance(
            rootPubkey,
            targetPubkey,
          );
          const endTime = performance.now();
          optimizedTimes.push(endTime - startTime);
          console.log(
            `Optimized: ${rootPubkey.substring(0, 8)}... -> ${targetPubkey.substring(0, 8)}...: ${distance} hops, ${(endTime - startTime).toFixed(2)}ms`,
          );
        }

        // Calculate statistics
        const baselineAvg =
          baselineTimes.reduce((a, b) => a + b, 0) / baselineTimes.length;
        const optimizedAvg =
          optimizedTimes.reduce((a, b) => a + b, 0) / optimizedTimes.length;

        console.log(`\nPerformance Comparison:`);
        console.log(
          `Baseline (bidirectional BFS): ${baselineAvg.toFixed(2)}ms avg`,
        );
        console.log(
          `Optimized (pre-computed): ${optimizedAvg.toFixed(2)}ms avg`,
        );

        if (baselineAvg > 0) {
          const speedup = baselineAvg / optimizedAvg;
          console.log(`Speedup: ${speedup.toFixed(1)}x faster`);
          console.log(
            `Time savings: ${(baselineAvg - optimizedAvg).toFixed(2)}ms per query`,
          );

          // On average, optimization should be faster
          expect(optimizedAvg).toBeLessThan(baselineAvg);
        }
      } finally {
        await analyzer.close();
      }
    }, 15000); // 30 second timeout

    it("should correctly handle rootPubkey optimization after graph changes", async () => {
      const analyzer = await DuckDBSocialGraphAnalyzer.create({
        maxDepth: 6,
      });

      try {
        // Create a simple test graph
        const { createMockKind3Event } = await import("./test-utils.js");

        // Simple chain: A -> B -> C
        const eventA = createMockKind3Event(
          "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          ["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"],
          1000,
        );
        const eventB = createMockKind3Event(
          "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
          ["cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"],
          1000,
        );

        await analyzer.ingestEvents([eventA, eventB]);

        // Set rootPubkey to A
        await analyzer.setRootPubkey(
          "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );

        // Verify optimization works
        const distance1 = await analyzer.getShortestDistance(
          "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        );
        expect(distance1).toBe(2);

        // Add a new edge that changes the graph: D -> A (making A harder to reach from C)
        const eventD = createMockKind3Event(
          "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
          ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
          1000,
        );
        await analyzer.ingestEvent(eventD);

        // Verify the optimization table was cleared and falls back to normal query
        const distance2 = await analyzer.getShortestDistance(
          "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        );
        expect(distance2).toBe(2); // Distance should still be the same

        // Set rootPubkey again to rebuild the table
        await analyzer.setRootPubkey(
          "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );

        // Verify optimization works again
        const distance3 = await analyzer.getShortestDistance(
          "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        );
        expect(distance3).toBe(2);
      } finally {
        await analyzer.close();
      }
    }, 15000); // 15 second timeout
  });
});
