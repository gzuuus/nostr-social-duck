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

describe("Performance Benchmarks", () => {
  let instance: DuckDBInstance;
  let connection: any;

  beforeEach(async () => {
    // Use the existing database with real data
    instance = await initializeDatabase("examples/social-graph.db");
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
        `Multi-hop path - getShortestPath: ${pathAvgTime.toFixed(2)}ms`,
      );
      console.log(
        `Multi-hop path - getShortestDistance: ${distanceAvgTime.toFixed(2)}ms`,
      );

      if (distanceAvgTime > 0) {
        console.log(
          `Performance ratio: ${(pathAvgTime / distanceAvgTime).toFixed(2)}x`,
        );
      }

      // For multi-hop paths, distance should be faster due to skipping path reconstruction
      if (pathAvgTime > 10 && distanceAvgTime > 0) {
        // Only check if queries are non-trivial
        expect(distanceAvgTime).toBeLessThan(pathAvgTime);
      }
    }, 10000); // 10 second timeout

    it("should handle direct connections efficiently", async () => {
      // Find some direct connections in the graph
      const nearbyUsers = await getUsersWithinDistance(
        connection,
        "6e468422dfb74a5738702a8823b9b28168abab8655faacb6853cd0ee15deee93",
        1,
      );

      if (nearbyUsers.length === 0) {
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
      expect(pathAvgTime).toBeLessThan(10);
      expect(distanceAvgTime).toBeLessThan(10);
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
});
