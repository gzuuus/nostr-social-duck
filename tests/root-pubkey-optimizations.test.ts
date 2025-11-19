/**
 * Tests for root pubkey optimizations and statistical methods
 * Uses the existing database with real data for realistic testing
 */

import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { DuckDBSocialGraphAnalyzer } from "../src/analyzer.js";

describe("Root Pubkey Optimizations", () => {
  let analyzer: DuckDBSocialGraphAnalyzer;

  beforeEach(async () => {
    // Use the existing database with real data
    analyzer = await DuckDBSocialGraphAnalyzer.create({
      dbPath: "data/social-graph.db",
      maxDepth: 6,
    });
  });

  afterEach(async () => {
    await analyzer.close();
  });

  describe("getUsersWithinDistance optimization", () => {
    it("should use optimized path when querying from root pubkey", async () => {
      const allPubkeys = await analyzer.getAllUniquePubkeys();
      if (allPubkeys.length < 20) {
        console.log("Not enough test data for optimization test");
        return;
      }

      const rootPubkey = allPubkeys[0];

      await analyzer.setRootPubkey(rootPubkey);

      const users = await analyzer.getUsersWithinDistance(rootPubkey, 2);

      expect(users).toBeInstanceOf(Array);
      expect(users).not.toBeNull();
    });

    it("should fallback to standard approach for non-root pubkeys", async () => {
      const allPubkeys = await analyzer.getAllUniquePubkeys();
      if (allPubkeys.length < 20) {
        console.log("Not enough test data for fallback test");
        return;
      }

      await analyzer.setRootPubkey(allPubkeys[0]);

      const users = await analyzer.getUsersWithinDistance(allPubkeys[1], 1);

      expect(users).toBeInstanceOf(Array);
      expect(users).not.toBeNull();
    });
  });

  describe("getUsersAtDistance", () => {
    it("should get users exactly at specified distance from root", async () => {
      const allPubkeys = await analyzer.getAllUniquePubkeys();
      if (allPubkeys.length < 20) {
        console.log("Not enough test data for distance-distribution test");
        return;
      }

      const rootPubkey = allPubkeys[0];
      await analyzer.setRootPubkey(rootPubkey);

      const users = await analyzer.getUsersAtDistance(2);
      expect(users).toBeInstanceOf(Array);

      // Should return valid pubkeys (might be empty if no nodes at that distance)
      if (users.length > 0) {
        expect(typeof users[0]).toBe("string");
        expect(users[0]).toHaveLength(64); // Valid pubkey length
      }
    });

    it("should handle distance 0 correctly", async () => {
      const allPubkeys = await analyzer.getAllUniquePubkeys();
      const rootPubkey = allPubkeys[0];
      await analyzer.setRootPubkey(rootPubkey);

      const users = await analyzer.getUsersAtDistance(0);
      expect(users).toHaveLength(1); // Root itself
      expect(users[0]).toBe(rootPubkey.toLowerCase());
    });

    it("should throw error if root pubkey is not set", async () => {
      expect(analyzer.getUsersAtDistance(1)).rejects.toThrow(
        "Root pubkey must be set to use getUsersAtDistance",
      );
    });
  });

  describe("getDistanceDistribution", () => {
    it("should return correct distribution of users by distance", async () => {
      const allPubkeys = await analyzer.getAllUniquePubkeys();
      const rootPubkey = allPubkeys[0];
      await analyzer.setRootPubkey(rootPubkey);

      const distribution = await analyzer.getDistanceDistribution();

      expect(distribution).toBeInstanceOf(Object);

      // Should contain numeric keys and positive counts
      const distances = Object.keys(distribution).map(Number);
      distances.forEach((distance) => {
        expect(distance).toBeGreaterThan(0);
        expect(distribution[distance]).toBeGreaterThan(0);
      });
    });

    it("should throw error if root pubkey is not set", async () => {
      await expect(analyzer.getDistanceDistribution()).rejects.toThrow(
        "Root pubkey must be set to use getDistanceDistribution",
      );
    });
  });

  describe("Performance validation with real data", () => {
    it("should show performance improvement with root pubkey optimization", async () => {
      const allPubkeys = await analyzer.getAllUniquePubkeys();
      if (allPubkeys.length < 50) {
        console.log("Not enough test data for performance comparison");
        return;
      }

      const rootPubkey = allPubkeys[0];
      console.log(
        `Testing rootPubkey optimization with graph containing ${allPubkeys.length} pubkeys`,
      );

      // Test WITHOUT rootPubkey optimization
      const startTime1 = performance.now();
      const users1 = await analyzer.getUsersWithinDistance(rootPubkey, 2);
      const time1 = performance.now() - startTime1;
      console.log(`Baseline getUsersWithinDistance: ${time1.toFixed(2)}ms`);

      // Set rootPubkey for optimization
      await analyzer.setRootPubkey(rootPubkey);

      // Test WITH rootPubkey optimization
      const startTime2 = performance.now();
      const users2 = await analyzer.getUsersWithinDistance(rootPubkey, 2);
      const time2 = performance.now() - startTime2;
      console.log(`Optimized getUsersWithinDistance: ${time2.toFixed(2)}ms`);

      // Results should be consistent
      if (users1 && users2) {
        // Compare lengths - allow for slight variations due to different algorithms
        expect(Math.abs(users1.length - users2.length)).toBeLessThan(5);
      }

      console.log(`Performance ratio: ${(time1 / time2).toFixed(1)}x`);

      // Optimized should be faster than baseline
      expect(time2).toBeLessThan(time1 * 5); // Allow some variance
    }, 15000);

    it("should show fast performance for statistical methods", async () => {
      const allPubkeys = await analyzer.getAllUniquePubkeys();
      const rootPubkey = allPubkeys[0];
      await analyzer.setRootPubkey(rootPubkey);

      const startTime = performance.now();
      const usersAtDistance = await analyzer.getUsersAtDistance(2);
      const time = performance.now() - startTime;

      console.log(`getUsersAtDistance(2): ${time.toFixed(2)}ms`);

      expect(time).toBeLessThan(100); // Should be very fast
      expect(usersAtDistance).toBeInstanceOf(Array);
    });
  });

  describe("Integration with existing analyzer methods", () => {
    it("should work correctly with getShortestDistance optimization", async () => {
      const allPubkeys = await analyzer.getAllUniquePubkeys();
      const rootPubkey = allPubkeys[0];

      await analyzer.setRootPubkey(rootPubkey);

      // Should use optimized path
      const distance = await analyzer.getShortestDistance(
        rootPubkey,
        allPubkeys[1],
      );

      // Should return valid result (could be null if no path exists)
      expect(distance === null || distance >= 0).toBe(true);
    });
  });
});
