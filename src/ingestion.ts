/**
 * Data ingestion functions for Nostr Kind 3 events
 */

import { DuckDBConnection } from "@duckdb/node-api";
import type { NostrEvent } from "./types.js";
import { parseKind3Event } from "./parser.js";

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

  // Delete existing follows from this pubkey (event replacement)
  await deleteFollowsForPubkey(connection, event.pubkey);

  // Deduplicate follows by (follower_pubkey, followed_pubkey, event_id)
  const uniqueFollows = Array.from(
    new Map(
      follows.map((f) => [
        `${f.follower_pubkey}:${f.followed_pubkey}:${f.event_id}`,
        f,
      ]),
    ).values(),
  );

  // Insert in chunks to avoid memory issues with very large follow lists
  const CHUNK_SIZE = 500;
  for (let i = 0; i < uniqueFollows.length; i += CHUNK_SIZE) {
    const chunk = uniqueFollows.slice(i, i + CHUNK_SIZE);
    const values = chunk.map(() => "(?, ?, ?, ?)").join(", ");
    const params: (string | number)[] = [];

    for (const follow of chunk) {
      params.push(
        follow.follower_pubkey,
        follow.followed_pubkey,
        follow.event_id,
        follow.created_at,
      );
    }

    await connection.run(
      `INSERT INTO nsd_follows (follower_pubkey, followed_pubkey, event_id, created_at) VALUES ${values}`,
      params,
    );
  }
}

/**
 * Ingests multiple Kind 3 Nostr events into the database
 *
 * Events are processed sequentially to handle replacements correctly.
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

  // Group events by pubkey and keep only the latest for each
  const latestEventsByPubkey = new Map<string, NostrEvent>();

  for (const event of events) {
    const existing = latestEventsByPubkey.get(event.pubkey);

    // Keep the event with the latest timestamp
    if (!existing || event.created_at > existing.created_at) {
      latestEventsByPubkey.set(event.pubkey, event);
    }
  }

  // Process each unique pubkey's latest event
  for (const event of latestEventsByPubkey.values()) {
    await ingestEvent(connection, event);
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
