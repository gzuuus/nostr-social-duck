/**
 * Tests for persistent root optimizations with delta updates
 */

import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { DuckDBSocialGraphAnalyzer } from "../src/analyzer.js";
import type { NostrEvent } from "../src/types.js";
import { TEST_PUBKEYS, createMockKind3Event } from "./test-utils.js";

describe("Persistent Root Optimizations", () => {
  let analyzer: DuckDBSocialGraphAnalyzer;

  beforeEach(async () => {
    analyzer = await DuckDBSocialGraphAnalyzer.create({
      dbPath: ":memory:",
      maxDepth: 6,
    });
  });

  afterEach(async () => {
    await analyzer.close();
  });

  it("should apply delta updates when ingesting new events", async () => {
    // Create initial events
    const initialEvents: NostrEvent[] = [
      createMockKind3Event(TEST_PUBKEYS.adam, [TEST_PUBKEYS.fiatjaf], 1000),
    ];

    // Set root and build initial table
    await analyzer.setRootPubkey(TEST_PUBKEYS.adam);
    await analyzer.ingestEvents(initialEvents);

    // Verify initial distances
    const initialDistance1 = await analyzer.getShortestDistance(
      TEST_PUBKEYS.adam,
      TEST_PUBKEYS.fiatjaf,
    );
    expect(initialDistance1).toBe(1);

    // Add a new event that extends the graph
    const newEvent: NostrEvent = createMockKind3Event(
      TEST_PUBKEYS.fiatjaf,
      [TEST_PUBKEYS.snowden],
      1001,
    );

    // Ingest the new event - should trigger delta update
    await analyzer.ingestEvent(newEvent);

    // Verify the delta update worked
    const updatedDistance2 = await analyzer.getShortestDistance(
      TEST_PUBKEYS.adam,
      TEST_PUBKEYS.snowden,
    );
    expect(updatedDistance2).toBe(2); // adam -> fiatjaf -> snowden

    // Verify existing distances are still correct
    const preservedDistance1 = await analyzer.getShortestDistance(
      TEST_PUBKEYS.adam,
      TEST_PUBKEYS.fiatjaf,
    );
    expect(preservedDistance1).toBe(1);
  });

  it("should handle batch delta updates correctly", async () => {
    // Ingest initial events first
    await analyzer.ingestEvents([
      createMockKind3Event(TEST_PUBKEYS.adam, [TEST_PUBKEYS.fiatjaf], 1000),
    ]);

    // Set root after initial ingestion
    await analyzer.setRootPubkey(TEST_PUBKEYS.adam);

    // Now ingest additional events - should trigger batch delta update
    const additionalEvents: NostrEvent[] = [
      createMockKind3Event(TEST_PUBKEYS.fiatjaf, [TEST_PUBKEYS.snowden], 1001),
      createMockKind3Event(TEST_PUBKEYS.adam, [TEST_PUBKEYS.sirius], 1002),
    ];

    await analyzer.ingestEvents(additionalEvents);

    // Verify all distances are correct
    const distance1 = await analyzer.getShortestDistance(
      TEST_PUBKEYS.adam,
      TEST_PUBKEYS.fiatjaf,
    );
    const distance2 = await analyzer.getShortestDistance(
      TEST_PUBKEYS.adam,
      TEST_PUBKEYS.snowden,
    );
    const distance3 = await analyzer.getShortestDistance(
      TEST_PUBKEYS.adam,
      TEST_PUBKEYS.sirius,
    );

    expect(distance1).toBe(1);
    expect(distance2).toBe(2);
    expect(distance3).toBe(1);
  });

  it("should provide explicit table management methods", async () => {
    // Set root and build table
    await analyzer.setRootPubkey(TEST_PUBKEYS.adam);

    await analyzer.ingestEvent(
      createMockKind3Event(TEST_PUBKEYS.adam, [TEST_PUBKEYS.fiatjaf], 1000),
    );

    // Test rebuild method
    await analyzer.rebuildRootDistances();

    // Verify table still works after rebuild
    const distance = await analyzer.getShortestDistance(
      TEST_PUBKEYS.adam,
      TEST_PUBKEYS.fiatjaf,
    );
    expect(distance).toBe(1);

    // Test drop method
    await analyzer.dropRootDistances();

    // Should throw error when trying to use root methods without table
    expect(async () => {
      await analyzer.getUsersAtDistance(1);
    }).toThrow("Root pubkey must be set to use getUsersAtDistance");

    // Rebuild the table
    await analyzer.setRootPubkey(TEST_PUBKEYS.adam);

    // Should work again
    const newDistance = await analyzer.getShortestDistance(
      TEST_PUBKEYS.adam,
      TEST_PUBKEYS.fiatjaf,
    );
    expect(newDistance).toBe(1);
  });

  it("should handle schema changes gracefully", async () => {
    // Set root with one max depth
    await analyzer.setRootPubkey(TEST_PUBKEYS.adam);

    await analyzer.ingestEvent(
      createMockKind3Event(TEST_PUBKEYS.adam, [TEST_PUBKEYS.fiatjaf], 1000),
    );

    // Change max depth - should trigger table rebuild
    analyzer.setMaxDepth(8);
    await analyzer.rebuildRootDistances();

    // Verify it still works
    const distance = await analyzer.getShortestDistance(
      TEST_PUBKEYS.adam,
      TEST_PUBKEYS.fiatjaf,
    );
    expect(distance).toBe(1);
  });
});
