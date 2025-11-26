/**
 * Data ingestion functions for Nostr Kind 3 events
 */

import { DuckDBConnection } from "@duckdb/node-api";
import type { NostrEvent } from "./types.js";
import { parseKind3Event, validateKind3Event } from "./parser.js";
import { executeWithRetry, isHexKey } from "./utils.js";

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
    // Validate event structure before processing
    validateKind3Event(event);

    const existing = latestEventsByPubkey.get(event.pubkey);

    // Keep the event with the latest timestamp
    if (!existing || event.created_at > existing.created_at) {
      latestEventsByPubkey.set(event.pubkey, event);
    }
  }

  console.log(
    `Processing ${latestEventsByPubkey.size} unique events after deduplication`,
  );

  // Process events in smaller batches for better memory management
  const BATCH_SIZE = 100;
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
 * Processes a batch of events with streaming buffer for memory efficiency
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

  // Collect pubkeys that need deletion (all events with follows)
  const pubkeysToDelete = new Set<string>();
  let hasFollows = false;

  // Quick scan to check if we have any follows in this batch
  // We manually check tags to avoid the overhead of full parsing/object creation
  for (const event of events) {
    if (!Array.isArray(event.tags)) continue;

    let hasValidFollow = false;
    for (const tag of event.tags) {
      // Check for valid 'p' tag: ["p", "hex_pubkey", ...]
      if (
        Array.isArray(tag) &&
        tag.length >= 2 &&
        tag[0] === "p" &&
        isHexKey(tag[1])
      ) {
        hasValidFollow = true;
        break;
      }
    }

    if (hasValidFollow) {
      pubkeysToDelete.add(event.pubkey);
      hasFollows = true;
    }
  }

  // Skip if no follows in this batch
  if (!hasFollows) {
    return;
  }

  // Phase 2: Database operations with streaming buffer
  await executeWithRetry(async () => {
    await connection.run("BEGIN TRANSACTION");

    try {
      // Bulk delete existing follows for all pubkeys in this batch
      const pubkeysArray = Array.from(pubkeysToDelete);
      await bulkDeleteFollows(connection, pubkeysArray);

      // Insert follows using streaming buffer
      const BUFFER_SIZE = 3000;
      const buffer: Array<{
        follower_pubkey: string;
        followed_pubkey: string;
        created_at: number;
      }> = [];

      const flushBuffer = async () => {
        if (buffer.length === 0) return;

        const placeholders = buffer.map(() => "(?, ?, ?)").join(", ");
        const params: (string | number)[] = [];

        for (const follow of buffer) {
          params.push(
            follow.follower_pubkey,
            follow.followed_pubkey,
            follow.created_at,
          );
        }

        await connection.run(
          `INSERT OR REPLACE INTO nsd_follows (follower_pubkey, followed_pubkey, created_at) VALUES ${placeholders}`,
          params,
        );

        buffer.length = 0; // Clear buffer efficiently
      };

      // Process events one by one with per-event deduplication
      for (const event of events) {
        // Skip validation here as it was done in ingestEvents
        const { follows } = parseKind3Event(event, true);
        if (follows.length === 0) continue;

        // Deduplicate follows within this event only
        const seenFollowed = new Set<string>();
        const eventFollows: typeof follows = [];

        for (const follow of follows) {
          if (!seenFollowed.has(follow.followed_pubkey)) {
            seenFollowed.add(follow.followed_pubkey);
            eventFollows.push(follow);
          }
        }

        // Add deduplicated follows to buffer
        for (const follow of eventFollows) {
          buffer.push(follow);

          // Flush buffer when it reaches the size limit
          if (buffer.length >= BUFFER_SIZE) {
            await flushBuffer();
          }
        }
      }

      // Flush any remaining data in buffer
      await flushBuffer();

      await connection.run("COMMIT");
    } catch (error) {
      await connection.run("ROLLBACK");
      throw error;
    }
  });
}
