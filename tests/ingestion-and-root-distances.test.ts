import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { DuckDBSocialGraphAnalyzer } from "../src/analyzer.js";
import { createMockKind3Event } from "./test-utils.js";

describe("Ingestion and Root Distances Integration", () => {
  let analyzer: DuckDBSocialGraphAnalyzer;

  beforeEach(async () => {
    analyzer = await DuckDBSocialGraphAnalyzer.create();
  });

  afterEach(async () => {
    await analyzer.close();
  });

  // Generate valid SHA256 hashes for test pubkeys
  const TEST_PUBKEYS = {
    root: "a".repeat(64),
    layer1a: "b".repeat(64),
    layer1b: "c".repeat(64),
    layer2a: "d".repeat(64),
    layer2b: "e".repeat(64),
    layer3a: "f".repeat(64),
    layer3b: "1".repeat(64),
  };

  it("should build root distances table correctly with iterative construction", async () => {
    // Build a multi-layer graph
    const events = [
      // Root follows layer1 nodes
      createMockKind3Event(
        TEST_PUBKEYS.root,
        [TEST_PUBKEYS.layer1a, TEST_PUBKEYS.layer1b],
        1000,
      ),
      // Layer1 nodes follow layer2 nodes
      createMockKind3Event(TEST_PUBKEYS.layer1a, [TEST_PUBKEYS.layer2a], 1001),
      createMockKind3Event(TEST_PUBKEYS.layer1b, [TEST_PUBKEYS.layer2b], 1002),
      // Layer2 nodes follow layer3 nodes
      createMockKind3Event(TEST_PUBKEYS.layer2a, [TEST_PUBKEYS.layer3a], 1003),
      createMockKind3Event(TEST_PUBKEYS.layer2b, [TEST_PUBKEYS.layer3b], 1004),
    ];

    // Ingest all events first
    await analyzer.ingestEvents(events);

    // Set root pubkey - this should trigger iterative layer-by-layer construction
    await analyzer.setRootPubkey(TEST_PUBKEYS.root);

    // Verify distances are correct for all layers
    const distance1a = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer1a,
    );
    const distance1b = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer1b,
    );
    const distance2a = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer2a,
    );
    const distance2b = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer2b,
    );
    const distance3a = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer3a,
    );
    const distance3b = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer3b,
    );

    expect(distance1a).toBe(1);
    expect(distance1b).toBe(1);
    expect(distance2a).toBe(2);
    expect(distance2b).toBe(2);
    expect(distance3a).toBe(3);
    expect(distance3b).toBe(3);
  });

  it("should handle delta updates after initial root table construction", async () => {
    // Build initial graph
    const initialEvents = [
      createMockKind3Event(TEST_PUBKEYS.root, [TEST_PUBKEYS.layer1a], 1000),
      createMockKind3Event(TEST_PUBKEYS.layer1a, [TEST_PUBKEYS.layer2a], 1001),
    ];

    await analyzer.ingestEvents(initialEvents);
    await analyzer.setRootPubkey(TEST_PUBKEYS.root);

    // Verify initial distances
    const initialDistance1a = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer1a,
    );
    const initialDistance2a = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer2a,
    );
    expect(initialDistance1a).toBe(1);
    expect(initialDistance2a).toBe(2);

    // Add new follow relationships - should trigger delta updates
    const newEvents = [
      createMockKind3Event(TEST_PUBKEYS.root, [TEST_PUBKEYS.layer1b], 1002),
      createMockKind3Event(TEST_PUBKEYS.layer1b, [TEST_PUBKEYS.layer2b], 1003),
    ];

    await analyzer.ingestEvents(newEvents);

    // Verify new distances are correct (delta updates should have worked)
    const updatedDistance1b = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer1b,
    );
    const updatedDistance2b = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer2b,
    );
    expect(updatedDistance1b).toBe(1);
    expect(updatedDistance2b).toBe(2);

    // Ensure existing distances are still correct
    const existingDistance1a = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer1a,
    );
    const existingDistance2a = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer2a,
    );
    expect(existingDistance1a).toBe(1);
    expect(existingDistance2a).toBe(2);
  });

  it("should handle complex graph with multiple paths to same node", async () => {
    // Create a graph where a node can be reached via multiple paths
    const events = [
      // Root follows A and B
      createMockKind3Event(
        TEST_PUBKEYS.root,
        [TEST_PUBKEYS.layer1a, TEST_PUBKEYS.layer1b],
        1000,
      ),
      // Both A and B follow C (creating multiple paths to C)
      createMockKind3Event(TEST_PUBKEYS.layer1a, [TEST_PUBKEYS.layer2a], 1001),
      createMockKind3Event(TEST_PUBKEYS.layer1b, [TEST_PUBKEYS.layer2a], 1002),
      // C follows D
      createMockKind3Event(TEST_PUBKEYS.layer2a, [TEST_PUBKEYS.layer3a], 1003),
    ];

    await analyzer.ingestEvents(events);
    await analyzer.setRootPubkey(TEST_PUBKEYS.root);

    // Verify the shortest path is used (distance 2 to layer2a, not 1)
    const distanceToLayer2a = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer2a,
    );
    const distanceToLayer3a = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer3a,
    );

    expect(distanceToLayer2a).toBe(2);
    expect(distanceToLayer3a).toBe(3);
  });

  it("should maintain root table consistency across analyzer instances", async () => {
    // Test with persistent database to ensure table survives across instances
    const dbPath = "./test-integration.db";

    // Create first analyzer with persistent database
    const analyzer1 = await DuckDBSocialGraphAnalyzer.create({ dbPath });

    try {
      // Build graph and set root
      const events = [
        createMockKind3Event(TEST_PUBKEYS.root, [TEST_PUBKEYS.layer1a], 1000),
        createMockKind3Event(
          TEST_PUBKEYS.layer1a,
          [TEST_PUBKEYS.layer2a],
          1001,
        ),
      ];

      await analyzer1.ingestEvents(events);
      await analyzer1.setRootPubkey(TEST_PUBKEYS.root);

      // Verify distances in first instance
      const distance1a = await analyzer1.getShortestDistance(
        TEST_PUBKEYS.root,
        TEST_PUBKEYS.layer1a,
      );
      const distance2a = await analyzer1.getShortestDistance(
        TEST_PUBKEYS.root,
        TEST_PUBKEYS.layer2a,
      );
      expect(distance1a).toBe(1);
      expect(distance2a).toBe(2);

      // Close first analyzer
      await analyzer1.close();

      // Create second analyzer with same database
      const analyzer2 = await DuckDBSocialGraphAnalyzer.create({ dbPath });

      try {
        // Set root again - should reuse existing table
        await analyzer2.setRootPubkey(TEST_PUBKEYS.root);

        // Verify distances are still correct (table was reused, not rebuilt)
        const distance1aAgain = await analyzer2.getShortestDistance(
          TEST_PUBKEYS.root,
          TEST_PUBKEYS.layer1a,
        );
        const distance2aAgain = await analyzer2.getShortestDistance(
          TEST_PUBKEYS.root,
          TEST_PUBKEYS.layer2a,
        );
        expect(distance1aAgain).toBe(1);
        expect(distance2aAgain).toBe(2);
      } finally {
        await analyzer2.close();
      }
    } finally {
      // Clean up test database
      try {
        await Bun.$`rm -f ${dbPath}`;
      } catch {
        // Ignore cleanup errors
      }
    }
  });

  it("should handle delta updates that create shorter paths through new connections", async () => {
    // Build initial graph with a longer path
    const initialEvents = [
      createMockKind3Event(TEST_PUBKEYS.root, [TEST_PUBKEYS.layer1a], 1000),
      createMockKind3Event(TEST_PUBKEYS.layer1a, [TEST_PUBKEYS.layer2a], 1001),
      createMockKind3Event(TEST_PUBKEYS.layer2a, [TEST_PUBKEYS.layer3a], 1002),
    ];

    await analyzer.ingestEvents(initialEvents);
    await analyzer.setRootPubkey(TEST_PUBKEYS.root);

    // Verify initial distances (path length 3)
    const initialDistance3a = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer3a,
    );
    expect(initialDistance3a).toBe(3);

    // Add a new direct connection that creates a shorter path
    const newEvents = [
      createMockKind3Event(TEST_PUBKEYS.layer1b, [TEST_PUBKEYS.layer3a], 1003),
      createMockKind3Event(TEST_PUBKEYS.root, [TEST_PUBKEYS.layer1b], 1004), // This should create a path of length 2
    ];

    await analyzer.ingestEvents(newEvents);

    // Verify the shorter path is found through delta updates
    const updatedDistance3a = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer3a,
    );
    expect(updatedDistance3a).toBe(2); // Now distance 2 via layer1b
  });

  it("should handle delta updates with multiple levels of propagation", async () => {
    // Build initial graph
    const initialEvents = [
      createMockKind3Event(TEST_PUBKEYS.root, [TEST_PUBKEYS.layer1a], 1000),
      createMockKind3Event(TEST_PUBKEYS.layer1a, [TEST_PUBKEYS.layer2a], 1001),
    ];

    await analyzer.ingestEvents(initialEvents);
    await analyzer.setRootPubkey(TEST_PUBKEYS.root);

    // Verify initial distances
    const initialDistance1a = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer1a,
    );
    const initialDistance2a = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer2a,
    );
    expect(initialDistance1a).toBe(1);
    expect(initialDistance2a).toBe(2);

    // Add events that require multi-level propagation
    const newEvents = [
      createMockKind3Event(TEST_PUBKEYS.layer2a, [TEST_PUBKEYS.layer3a], 1002), // This should propagate to layer3a
      createMockKind3Event(TEST_PUBKEYS.layer3a, [TEST_PUBKEYS.layer3b], 1003), // This should propagate to layer3b
    ];

    await analyzer.ingestEvents(newEvents);

    // Verify multi-level propagation worked
    const distance3a = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer3a,
    );
    const distance3b = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer3b,
    );
    expect(distance3a).toBe(3);
    expect(distance3b).toBe(4);
  });

  it("should handle delta updates with batch ingestion of multiple events", async () => {
    // Build initial graph
    const initialEvents = [
      createMockKind3Event(TEST_PUBKEYS.root, [TEST_PUBKEYS.layer1a], 1000),
    ];

    await analyzer.ingestEvents(initialEvents);
    await analyzer.setRootPubkey(TEST_PUBKEYS.root);

    // Verify initial distance
    const initialDistance1a = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer1a,
    );
    expect(initialDistance1a).toBe(1);

    // Add multiple events in a single batch that create a complex subgraph
    const batchEvents = [
      createMockKind3Event(
        TEST_PUBKEYS.layer1a,
        [TEST_PUBKEYS.layer2a, TEST_PUBKEYS.layer2b],
        1001,
      ),
      createMockKind3Event(TEST_PUBKEYS.layer2a, [TEST_PUBKEYS.layer3a], 1002),
      createMockKind3Event(TEST_PUBKEYS.layer2b, [TEST_PUBKEYS.layer3b], 1003),
      createMockKind3Event(TEST_PUBKEYS.layer3a, [TEST_PUBKEYS.layer3b], 1004), // Creates an alternative path
    ];

    await analyzer.ingestEvents(batchEvents);

    // Verify all distances are correctly updated
    const distance2a = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer2a,
    );
    const distance2b = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer2b,
    );
    const distance3a = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer3a,
    );
    const distance3b = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer3b,
    );

    expect(distance2a).toBe(2);
    expect(distance2b).toBe(2);
    expect(distance3a).toBe(3);
    expect(distance3b).toBe(3); // Should find the shortest path (via layer2a or layer2b, not via layer3a)
  });

  it("should handle delta updates that don't affect existing distances", async () => {
    // Build initial graph
    const initialEvents = [
      createMockKind3Event(TEST_PUBKEYS.root, [TEST_PUBKEYS.layer1a], 1000),
      createMockKind3Event(TEST_PUBKEYS.layer1a, [TEST_PUBKEYS.layer2a], 1001),
    ];

    await analyzer.ingestEvents(initialEvents);
    await analyzer.setRootPubkey(TEST_PUBKEYS.root);

    // Verify initial distances
    const initialDistance1a = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer1a,
    );
    const initialDistance2a = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer2a,
    );
    expect(initialDistance1a).toBe(1);
    expect(initialDistance2a).toBe(2);

    // Add events that don't create shorter paths
    const newEvents = [
      createMockKind3Event(TEST_PUBKEYS.layer2a, [TEST_PUBKEYS.layer3a], 1002), // This extends the graph but doesn't affect existing nodes
    ];

    await analyzer.ingestEvents(newEvents);

    // Verify existing distances remain unchanged
    const existingDistance1a = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer1a,
    );
    const existingDistance2a = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer2a,
    );
    expect(existingDistance1a).toBe(1);
    expect(existingDistance2a).toBe(2);

    // Verify new node has correct distance
    const distance3a = await analyzer.getShortestDistance(
      TEST_PUBKEYS.root,
      TEST_PUBKEYS.layer3a,
    );
    expect(distance3a).toBe(3);
  });
});
