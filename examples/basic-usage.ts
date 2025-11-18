/**
 * Basic usage example for the Nostr Social Graph Analysis Library
 *
 * This example demonstrates:
 * 1. Creating an analyzer instance
 * 2. Loading events from a JSONL file
 * 3. Finding shortest paths between pubkeys
 * 4. Getting graph statistics
 */

import { DuckDBSocialGraphAnalyzer } from "../src/index.js";
import type { NostrEvent } from "../src/index.js";
import { readFileSync } from "fs";
import { join } from "path";

async function main() {
  console.log("🦆 Nostr Social Graph Analysis - Basic Example\n");

  // Create an in-memory analyzer with reasonable maxDepth for demo
  console.log("Creating analyzer...");
  const analyzer = await DuckDBSocialGraphAnalyzer.create({
    dbPath: "data/social-graph.db",
  });

  try {
    // Load events from the sample data file
    console.log("Loading events from socialGraph.jsonl...");
    const dataPath = join(process.cwd(), "data", "socialGraph.jsonl");
    const fileContent = readFileSync(dataPath, "utf-8");

    // Parse JSONL (one JSON object per line)
    const events: NostrEvent[] = fileContent
      .split("\n")
      .filter((line) => line.trim())
      .map((line) => JSON.parse(line));

    console.log(`Found ${events.length} events\n`);

    // // Ingest all events
    console.log("Ingesting events into the graph...");
    await analyzer.ingestEvents(events);
    console.log("✓ Events ingested successfully\n");

    // Get and display statistics
    console.log("Graph Statistics:");
    const stats = await analyzer.getStats();
    console.log(`  Total follow relationships: ${stats.totalFollows}`);
    console.log(`  Unique followers: ${stats.uniqueFollowers}`);
    console.log(`  Unique followed pubkeys: ${stats.uniqueFollowed}`);
    console.log(`  Unique events processed: ${stats.uniqueEvents}\n`);

    // Example: Find shortest path between two pubkeys
    if (events.length >= 2) {
      const fromPubkey =
        "6e468422dfb74a5738702a8823b9b28168abab8655faacb6853cd0ee15deee93";
      const toPubkey =
        "ee07e263a68afd3c9cc25bec9bde31c25b4156e71466bb3df9185be11be01122";

      console.log("Finding shortest path (with timeout):");
      console.log(`  From: ${fromPubkey.substring(0, 16)}...`);
      console.log(`  To:   ${toPubkey.substring(0, 16)}...`);
      console.log(`  Max depth: 3 hops`);

      try {
        // Add timeout to prevent hanging on disconnected nodes
        const pathPromise = analyzer.getShortestPath(fromPubkey, toPubkey, 6);
        const timeoutPromise = new Promise<null>((_, reject) =>
          setTimeout(
            () => reject(new Error("Query timeout after 10 seconds")),
            10000,
          ),
        );

        const path = await Promise.race([pathPromise, timeoutPromise]);

        if (path) {
          console.log(`\n✓ Path found!`);
          console.log(`  Distance: ${path.distance} hops`);
          console.log(`  Path:`);
          path.path.forEach((pubkey, index) => {
            const prefix = index === 0 ? "    →" : "    →";
            console.log(`${prefix} ${pubkey.substring(0, 16)}...`);
          });
        } else {
          console.log("\n✗ No path found within maximum depth");
        }
      } catch (error) {
        if (error instanceof Error && error.message.includes("timeout")) {
          console.log("\n⏱ Query timed out - nodes may not be connected");
        } else {
          throw error;
        }
      }
    }
  } catch (error) {
    console.error("Error:", error);
    throw error;
  } finally {
    // Always close the analyzer when done
    console.log("\nClosing analyzer...");
    await analyzer.close();
    console.log("✓ Done!");
    process.exit(0);
  }
}

// Run the example
main().catch(console.error);
