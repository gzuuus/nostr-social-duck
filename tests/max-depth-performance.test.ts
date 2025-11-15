/**
 * Performance tests for max depth configuration
 *
 * This test measures how different maxDepth values affect performance
 * for the same graph traversal computation.
 */

import { describe, it, expect } from "bun:test";
import { DuckDBSocialGraphAnalyzer } from "../src/analyzer.js";
import { createComplexGraph, measureTime, TEST_PUBKEYS } from "./test-utils.js";

describe("Max Depth Performance", () => {
  it("should measure performance impact of different maxDepth values", async () => {
    // Create test data
    const events = createComplexGraph();

    // Test different maxDepth configurations
    const maxDepthConfigs = [2, 4, 6, 8, 10];
    const results: Array<{
      maxDepth: number;
      time: number;
      pathFound: boolean;
    }> = [];

    // Define the computation to test - finding path between two specific nodes
    const fromPubkey = TEST_PUBKEYS.adam;
    const toPubkey = TEST_PUBKEYS.snowden;

    for (const maxDepth of maxDepthConfigs) {
      // Create analyzer with specific maxDepth
      const analyzer = await DuckDBSocialGraphAnalyzer.create({ maxDepth });

      try {
        // Ingest the same events for each test
        await analyzer.ingestEvents(events);

        // Measure time for the same shortest path computation
        const { result: path, time } = await measureTime(() =>
          analyzer.getShortestPath(fromPubkey, toPubkey),
        );

        results.push({
          maxDepth,
          time,
          pathFound: path !== null,
        });

        console.log(
          `maxDepth=${maxDepth}: ${time.toFixed(2)}ms, pathFound=${path !== null}`,
        );
      } finally {
        await analyzer.close();
      }
    }

    // Verify that all configurations found the path (or at least some did)
    const pathsFound = results.filter((r) => r.pathFound).length;
    expect(pathsFound).toBeGreaterThan(0);

    // Verify that we have results for all configurations
    expect(results).toHaveLength(maxDepthConfigs.length);

    // Log performance comparison
    console.log("\nPerformance Comparison:");
    results.forEach(({ maxDepth, time, pathFound }) => {
      console.log(
        `  maxDepth=${maxDepth}: ${time.toFixed(2)}ms, pathFound=${pathFound}`,
      );
    });

    // Basic assertion that the test ran successfully
    expect(results.length).toBeGreaterThan(0);
  });

  it("should show how maxDepth affects path discovery", async () => {
    // Create a more complex graph with longer paths
    const events = createComplexGraph();

    const testCases = [
      { from: TEST_PUBKEYS.adam, to: TEST_PUBKEYS.sirius, expectedMinDepth: 2 }, // adam -> sirius (via fiatjaf)
      {
        from: TEST_PUBKEYS.snowden,
        to: TEST_PUBKEYS.sirius,
        expectedMinDepth: 2,
      }, // snowden -> sirius (via adam -> fiatjaf)
    ];

    for (const testCase of testCases) {
      console.log(
        `\nTesting path from ${testCase.from.substring(0, 8)}... to ${testCase.to.substring(0, 8)}...`,
      );

      const depthResults: Array<{
        maxDepth: number;
        pathFound: boolean;
        distance?: number;
      }> = [];

      for (const maxDepth of [1, 2, 3, 4, 5, 6]) {
        const analyzer = await DuckDBSocialGraphAnalyzer.create({ maxDepth });

        try {
          await analyzer.ingestEvents(events);
          const path = await analyzer.getShortestPath(
            testCase.from,
            testCase.to,
          );

          depthResults.push({
            maxDepth,
            pathFound: path !== null,
            distance: path?.distance,
          });

          console.log(
            `  maxDepth=${maxDepth}: ${path ? `found (distance=${path.distance})` : "not found"}`,
          );
        } finally {
          await analyzer.close();
        }
      }

      // Verify that path is found when maxDepth is sufficient
      const sufficientDepth = depthResults.find(
        (r) => r.pathFound && r.maxDepth >= testCase.expectedMinDepth,
      );
      expect(sufficientDepth).toBeDefined();

      // Verify that path is not found when maxDepth is insufficient
      const insufficientDepth = depthResults.find(
        (r) => !r.pathFound && r.maxDepth < testCase.expectedMinDepth,
      );
      if (testCase.expectedMinDepth > 1) {
        expect(insufficientDepth).toBeDefined();
      }
    }
  });
});
