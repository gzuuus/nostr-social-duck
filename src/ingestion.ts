/**
 * Data ingestion functions for Nostr Kind 3 events
 */

import { DuckDBConnection } from "@duckdb/node-api";
import type { NostrEvent, FollowRelationship } from "./types.js";
import { parseKind3Event } from "./parser.js";

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
  await connection.run(
    `DELETE FROM nsd_follows WHERE follower_pubkey IN (${placeholders})`,
    pubkeys,
  );
}

/**
 * Ingests a single Kind 3 Nostr event into the database
 *
 * This implements Nostr's "latest event wins" semantics:
 * 1. Delete all existing follows for this pubkey
 * 2. Insert the new follow list from the event
 *
 * @param connection - Active DuckDB connection
 * @param event - The Nostr Kind 3 event to ingest
 */
export async function ingestEvent(
  connection: DuckDBConnection,
  event: NostrEvent,
): Promise<void> {
  // Parse the event to extract follow relationships
  const { follows } = parseKind3Event(event);

  // If there are no follows in this event, we're done
  if (follows.length === 0) {
    return;
  }

  // Use transaction for atomic operations
  await connection.run("BEGIN TRANSACTION");

  try {
    // Delete existing follows from this pubkey (event replacement)
    await deleteFollowsForPubkey(connection, event.pubkey);

    // Use Set for deduplication (more memory efficient than Map for this use case)
    const seenKeys = new Set<string>();
    const uniqueFollows: FollowRelationship[] = [];

    // Deduplicate follows by (follower_pubkey, followed_pubkey) only
    // Since primary key is now (follower_pubkey, followed_pubkey)
    for (const follow of follows) {
      const key = `${follow.follower_pubkey}:${follow.followed_pubkey}`;
      if (!seenKeys.has(key)) {
        seenKeys.add(key);
        uniqueFollows.push(follow);
      }
    }

    // Insert all follows in optimized chunks
    const CHUNK_SIZE = 500;
    for (let i = 0; i < uniqueFollows.length; i += CHUNK_SIZE) {
      const chunk = uniqueFollows.slice(i, i + CHUNK_SIZE);
      const values = chunk.map(() => "(?, ?, ?)").join(", ");
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
  const BATCH_SIZE = 1000;
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

  // Use transaction for atomic operations
  await connection.run("BEGIN TRANSACTION");

  try {
    // Process follows with minimal memory allocations
    const seenKeys = new Set<string>();
    const uniqueFollows: Array<{
      follower_pubkey: string;
      followed_pubkey: string;
      created_at: number;
    }> = [];
    const pubkeysToDelete = new Set<string>();

    // Single pass through events with simultaneous parsing and deduplication
    for (const event of events) {
      const { follows } = parseKind3Event(event);
      if (follows.length > 0) {
        pubkeysToDelete.add(event.pubkey);

        // Deduplicate and collect follows simultaneously
        // Deduplicate by (follower_pubkey, followed_pubkey) only
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
      await connection.run("COMMIT");
      return;
    }

    // Bulk delete existing follows for all pubkeys in this batch
    const pubkeysArray = Array.from(pubkeysToDelete);
    await bulkDeleteFollows(connection, pubkeysArray);

    // Insert all follows in optimized chunks
    const CHUNK_SIZE = 500;
    for (let i = 0; i < uniqueFollows.length; i += CHUNK_SIZE) {
      const chunk = uniqueFollows.slice(i, i + CHUNK_SIZE);
      const values = chunk.map(() => "(?, ?, ?)").join(", ");
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
}

/**
 * Deletes all follows for a specific pubkey
 *
 * @param connection - Active DuckDB connection
 * @param pubkey - The pubkey whose follows should be deleted
 */
export async function deleteFollowsForPubkey(
  connection: DuckDBConnection,
  pubkey: string,
): Promise<void> {
  await connection.run("DELETE FROM nsd_follows WHERE follower_pubkey = ?", [
    pubkey,
  ]);
}
