/**
 * Simple test to verify basic functionality
 */

import { DuckDBSocialGraphAnalyzer } from "../src/index.js";
import type { NostrEvent } from "../src/index.js";
import { readFileSync } from "fs";
import { join } from "path";

async function main() {
  console.log("🦆 Nostr Social Graph Analysis - Simple Test\n");

  const analyzer = await DuckDBSocialGraphAnalyzer.create({
    maxDepth: 3, // Limit depth for faster testing
  });

  try {
    // Load events
    console.log("Loading events...");
    const dataPath = join(process.cwd(), "data", "socialGraph.jsonl");
    const fileContent = readFileSync(dataPath, "utf-8");
    const events: NostrEvent[] = fileContent
      .split("\n")
      .filter((line) => line.trim())
      .map((line) => JSON.parse(line));

    console.log(`Found ${events.length} events\n`);

    // Ingest events
    console.log("Ingesting events...");
    await analyzer.ingestEvents(events);
    console.log("✓ Events ingested\n");

    // Get statistics
    const stats = await analyzer.getStats();
    console.log("Graph Statistics:");
    console.log(`  Total follows: ${stats.totalFollows}`);
    console.log(`  Unique followers: ${stats.uniqueFollowers}`);
    console.log(`  Unique followed: ${stats.uniqueFollowed}\n`);

    // Test 1: Same pubkey (should return distance 0)
    console.log("Test 1: Path from pubkey to itself");
    const pubkey1 = events[0]!.pubkey;
    console.log(`  Pubkey: ${pubkey1.substring(0, 16)}...`);
    const path1 = await analyzer.getShortestPath(pubkey1, pubkey1);
    if (path1 && path1.distance === 0) {
      console.log("  ✓ PASS: Distance is 0\n");
    } else {
      console.log("  ✗ FAIL: Expected distance 0\n");
    }

    // Test 2: Check if first pubkey follows anyone
    console.log("Test 2: Direct follow relationship");
    const follower = events[0]!.pubkey;
    const followedList = events[0]!.tags
      .filter((tag) => tag[0] === "p")
      .map((tag) => tag[1]!);

    if (followedList.length > 0) {
      const followed = followedList[0]!;
      console.log(`  From: ${follower.substring(0, 16)}...`);
      console.log(`  To:   ${followed.substring(0, 16)}...`);

      const path2 = await analyzer.getShortestPath(follower, followed, 2);
      if (path2) {
        console.log(`  ✓ PASS: Found path with distance ${path2.distance}`);
        console.log(
          `  Path: ${path2.path.map((p) => p.substring(0, 8)).join(" → ")}\n`,
        );
      } else {
        console.log("  ✗ FAIL: No path found\n");
      }
    } else {
      console.log("  ⊘ SKIP: No follows in first event\n");
    }

    // Test 3: Try to find a 2-hop path
    console.log("Test 3: Two-hop path (if exists)");
    if (events.length >= 2) {
      const from = events[0]!.pubkey;
      const to = events[1]!.pubkey;
      console.log(`  From: ${from.substring(0, 16)}...`);
      console.log(`  To:   ${to.substring(0, 16)}...`);
      console.log("  Searching with max depth 2...");

      const path3 = await analyzer.getShortestPath(from, to, 2);
      if (path3) {
        console.log(`  ✓ Found path with distance ${path3.distance}`);
        console.log(
          `  Path: ${path3.path.map((p) => p.substring(0, 8)).join(" → ")}\n`,
        );
      } else {
        console.log("  ⊘ No path found within depth 2 (this is OK)\n");
      }
    }

    console.log("✓ All tests completed!");
  } catch (error) {
    console.error("Error:", error);
    throw error;
  } finally {
    await analyzer.close();
  }
}

main().catch(console.error);
