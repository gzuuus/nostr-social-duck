/**
 * Data ingestion functions for Nostr Kind 3 events
 */

import { DuckDBConnection } from "@duckdb/node-api";
import type { NostrEvent, FollowRelationship } from "./types.js";
import { parseKind3Event } from "./parser.js";
import { executeWithRetry } from "./utils.js";

/**
 * Bulk deletes follows for multiple pubkeys using a single query
 */
async function bulkDeleteFollows(
  connection: DuckDBConnection,
  pubkeys: string[],
): Promise<void> {
  if (pubkeys.length === 0) {
    return;
  }

  // Use a single query with IN clause for bulk deletion
  const placeholders = pubkeys.map(() => "?").join(", ");
  await executeWithRetry(async () => {
    await connection.run(
      `DELETE FROM nsd_follows WHERE follower_pubkey IN (${placeholders})`,
      pubkeys,
    );
  });
}

/**
 * Ingests a single Kind 3 Nostr event into the database
 *
 * This implements Nostr's "latest event wins" semantics by delegating
 * to the batch ingestion function for consistent behavior.
 *
 * @param connection - Active DuckDB connection
 * @param event - The Nostr Kind 3 event to ingest
 */
export async function ingestEvent(
  connection: DuckDBConnection,
  event: NostrEvent,
): Promise<void> {
  // Delegate to batch ingestion for consistent processing
  await ingestEvents(connection, [event]);
}

/**
 * Ingests multiple Kind 3 Nostr events into the database
 *
 * Events are processed in batches to handle replacements efficiently.
 * If multiple events exist for the same pubkey, only the latest one
 * (by created_at timestamp) will be retained.
 *
 * @param connection - Active DuckDB connection
 * @param events - Array of Nostr Kind 3 events to ingest
 */
export async function ingestEvents(
  connection: DuckDBConnection,
  events: NostrEvent[],
): Promise<void> {
  if (events.length === 0) {
    return;
  }

  console.log(`Starting ingestion of ${events.length} events...`);

  // Group events by pubkey and keep only the latest for each
  const latestEventsByPubkey = new Map<string, NostrEvent>();

  for (const event of events) {
    const existing = latestEventsByPubkey.get(event.pubkey);

    // Keep the event with the latest timestamp
    if (!existing || event.created_at > existing.created_at) {
      latestEventsByPubkey.set(event.pubkey, event);
    }
  }

  console.log(
    `Processing ${latestEventsByPubkey.size} unique events after deduplication`,
  );

  // Process events in batches for better performance
  const BATCH_SIZE = 250;
  const latestEvents = Array.from(latestEventsByPubkey.values());
  const totalBatches = Math.ceil(latestEvents.length / BATCH_SIZE);

  for (let i = 0; i < latestEvents.length; i += BATCH_SIZE) {
    const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
    const batch = latestEvents.slice(i, i + BATCH_SIZE);
    console.log(
      `Processing batch ${batchNumber}/${totalBatches} (${batch.length} events)...`,
    );

    const startTime = performance.now();
    await processEventBatch(connection, batch);
    const endTime = performance.now();

    console.log(
      `Batch ${batchNumber} completed in ${(endTime - startTime).toFixed(2)}ms`,
    );
  }

  console.log(
    `Ingestion completed: ${latestEventsByPubkey.size} events processed`,
  );

  // Update metadata to track graph changes
  await executeWithRetry(async () => {
    await connection.run(
      `INSERT OR REPLACE INTO nsd_metadata (key, value) VALUES ('graph_updated_at', ?)`,
      [String(Date.now())],
    );
  });
}

/**
 * Processes a batch of events with optimized bulk operations
 *
 * @param connection - Active DuckDB connection
 * @param events - Batch of events to process
 */
async function processEventBatch(
  connection: DuckDBConnection,
  events: NostrEvent[],
): Promise<void> {
  if (events.length === 0) {
    return;
  }

  // Phase 1: Parse and prepare data outside transaction (CPU-bound)
  const seenKeys = new Set<string>();
  const uniqueFollows: Array<{
    follower_pubkey: string;
    followed_pubkey: string;
    created_at: number;
  }> = [];
  const pubkeysToDelete = new Set<string>();

  try {
    // Single pass through events with parsing and deduplication
    for (const event of events) {
      const { follows } = parseKind3Event(event);
      if (follows.length > 0) {
        pubkeysToDelete.add(event.pubkey);

        // Deduplicate and collect follows simultaneously
        for (const follow of follows) {
          const key = `${follow.follower_pubkey}:${follow.followed_pubkey}`;
          if (!seenKeys.has(key)) {
            seenKeys.add(key);
            uniqueFollows.push({
              follower_pubkey: follow.follower_pubkey,
              followed_pubkey: follow.followed_pubkey,
              created_at: follow.created_at,
            });
          }
        }
      }
    }

    // Skip if no follows in this batch
    if (uniqueFollows.length === 0) {
      return;
    }

    // Phase 2: Database operations (I/O-bound within transaction)
    await executeWithRetry(async () => {
      await connection.run("BEGIN TRANSACTION");

      try {
        // Bulk delete existing follows for all pubkeys in this batch
        const pubkeysArray = Array.from(pubkeysToDelete);
        await bulkDeleteFollows(connection, pubkeysArray);

        // Insert all follows in optimized chunks
        const CHUNK_SIZE = 250;

        // Pre-compute placeholders for maximum chunk size
        const maxPlaceholders = Array(CHUNK_SIZE).fill("(?, ?, ?)").join(", ");

        for (let i = 0; i < uniqueFollows.length; i += CHUNK_SIZE) {
          const chunk = uniqueFollows.slice(i, i + CHUNK_SIZE);

          // Use pre-computed placeholders for full chunks, custom for last chunk
          const values =
            chunk.length === CHUNK_SIZE
              ? maxPlaceholders
              : chunk.map(() => "(?, ?, ?)").join(", ");

          const params: (string | number)[] = [];

          for (const follow of chunk) {
            params.push(
              follow.follower_pubkey,
              follow.followed_pubkey,
              follow.created_at,
            );
          }

          await connection.run(
            `INSERT OR REPLACE INTO nsd_follows (follower_pubkey, followed_pubkey, created_at) VALUES ${values}`,
            params,
          );
        }

        await connection.run("COMMIT");
      } catch (error) {
        await connection.run("ROLLBACK");
        throw error;
      }
    });
  } finally {
    // Free memory by clearing references (help garbage collection)
    // This ensures cleanup happens even if there's an error
    seenKeys.clear();
    uniqueFollows.length = 0;
    pubkeysToDelete.clear();
  }
}
