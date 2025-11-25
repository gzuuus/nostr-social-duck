# Nostr Social Duck 🦆

<div>
  <img src="https://github.com/user-attachments/assets/a3a81410-0d2c-4b0d-93f5-43b15aef42c1" alt="Image" style="display:block;width:280px;height:auto;object-fit:contain;">
</div>
A high-performance TypeScript library for analyzing Nostr social graphs using DuckDB. Efficiently compute shortest distances and analyze follow relationships from Nostr Kind 3 events.

## Features

- 🚀 **Fast Graph Traversal** - Uses DuckDB's recursive CTEs with `USING KEY` optimization
- 📊 **Efficient Storage** - Optimized schema with strategic indexes
- 🔄 **NIP-02 Compliant** - Properly implements "latest event wins" semantics
- 💾 **Flexible Storage** - In-memory or persistent database options
- 🎯 **Type-Safe** - Full TypeScript support with comprehensive types
- ⚡ **Root Pubkey Optimization** - Pre-computed distances for O(1) lookups with persistent tables
- 🔄 **Delta Updates** - Incremental updates to root distances without full rebuilds
- 🔄 **Batch Operations** - Efficient multi-pubkey distance calculations
- 📈 **Advanced Analytics** - Degree analysis, mutual follows, distance distributions
- 💾 **Persistent Tables** - Root distances survive analyzer sessions in persistent databases

## Installation

```bash
bun add nostr-social-duck
# or
npm install nostr-social-duck
```

## Quick Start

```typescript
import { DuckDBSocialGraphAnalyzer } from "nostr-social-duck";

// Create analyzer (in-memory)
const analyzer = await DuckDBSocialGraphAnalyzer.create();

// Ingest Nostr Kind 3 events
await analyzer.ingestEvents(kind3Events);

// Find shortest path between two pubkeys
const path = await analyzer.getShortestPath("pubkey1...", "pubkey2...");

if (path) {
  console.log(`Distance: ${path.distance} hops`);
  console.log(`Path: ${path.path.join(" → ")}`);
}

// Get all users within 2 hops
const nearbyUsers = await analyzer.getUsersWithinDistance("pubkey1...", 2);
console.log(`Users within 2 hops: ${nearbyUsers.length}`);

// Get graph statistics
const stats = await analyzer.getStats();
console.log(`Total follows: ${stats.totalFollows}`);

// Clean up (automatic if using 'await using' syntax)
await analyzer.close();
```

## Advanced Usage with Root Pubkey Optimization

For applications that frequently query distances from a specific pubkey (like your own), use the root pubkey optimization for O(1) lookups:

```typescript
// Set the root pubkey
await analyzer.setRootPubkey("root_pubkey...");

// Now distance queries from your pubkey are extremely fast
const distance = await analyzer.getShortestDistance(
  "your_pubkey...",
  "target_pubkey...",
);
// O(1) lookup when querying from the root

// Get all users exactly 2 hops away
const usersAtDistance = await analyzer.getUsersAtDistance(2);

// Get distance distribution from your pubkey
const distribution = await analyzer.getDistanceDistribution();
console.log(distribution); // {1: 150, 2: 2500, 3: 12000, ...}
```

### Persistent Root Tables and Delta Updates

The root optimization now uses **persistent tables** that survive analyzer sessions when using persistent databases. This means:

- **Faster restarts**: Root tables are reused if the same root pubkey and maxDepth are configured
- **Delta updates**: When new events are ingested, the root distances table is incrementally updated instead of being rebuilt from scratch
- **Explicit management**: You have full control over when to rebuild or drop the root table

```typescript
// The root table is automatically reused when reconnecting to the same database
const analyzer1 = await DuckDBSocialGraphAnalyzer.create({
  dbPath: "./social-graph.db",
  rootPubkey: "your_pubkey...",
});

// Later, when creating a new analyzer with the same database
const analyzer2 = await DuckDBSocialGraphAnalyzer.create({
  dbPath: "./social-graph.db",
  rootPubkey: "your_pubkey...", // Table will be reused, not rebuilt
});

// Explicit table management
await analyzer.rebuildRootDistances(); // Force full rebuild
await analyzer.dropRootDistances(); // Remove the table completely
```

### Delta Update Performance

Delta updates provide significant performance improvements for incremental data ingestion:

```typescript
// Initial setup
await analyzer.setRootPubkey("your_pubkey...");
await analyzer.ingestEvents(initialEvents); // Builds root table

// Subsequent updates use delta updates (much faster)
await analyzer.ingestEvents(newEvents); // Incrementally updates root table
await analyzer.ingestEvent(singleEvent); // Also uses delta updates
```

## API Reference

### Creating an Analyzer

```typescript
// In-memory database (default)
const analyzer = await DuckDBSocialGraphAnalyzer.create();

// Persistent database
const analyzer = await DuckDBSocialGraphAnalyzer.create({
  dbPath: "./social-graph.db",
});

// With root pubkey optimization from the start
const analyzer = await DuckDBSocialGraphAnalyzer.create({
  rootPubkey: "your_pubkey...",
});
```

### Using Existing DuckDB Connections

```typescript
// Connect to existing DuckDB instance
const connection = await myInstance.connect();
const analyzer = await DuckDBSocialGraphAnalyzer.connect(connection);
```

**Note:** When using `connect()`, the analyzer won't close the connection when you call `close()`, allowing you to reuse the connection for other purposes.

### Using the `await using` Syntax for Automatic Cleanup

```typescript
// Using modern await using syntax for automatic resource cleanup
await using analyzer = await DuckDBSocialGraphAnalyzer.create();

// Use the analyzer - no need to call close() manually
await analyzer.ingestEvents(events);
const path = await analyzer.getShortestPath("pubkey1...", "pubkey2...");
// Connection automatically closed when analyzer goes out of scope
```

### Ingesting Events

```typescript
// Single event
await analyzer.ingestEvent(kind3Event);

// Multiple events (automatically deduplicates by pubkey)
await analyzer.ingestEvents([event1, event2, event3]);
```

### Finding Paths and Distances

```typescript
// Find shortest path (returns full path details)
const path = await analyzer.getShortestPath(fromPubkey, toPubkey);

// Returns: { path: string[], distance: number } | null

// Find shortest distance only (2-3x faster)
const distance = await analyzer.getShortestDistance(fromPubkey, toPubkey);

// Returns: number | null - the distance in hops, or null if no path exists

// Batch distance calculations (optimized for multiple targets)
const distances = await analyzer.getShortestDistancesBatch(fromPubkey, [
  "target1...",
  "target2...",
  "target3...",
]);
// Returns: Map<string, number | null> - map of target pubkey -> distance
```

### Finding Users Within Distance

```typescript
// Get all users within specified distance
const users = await analyzer.getUsersWithinDistance(
  fromPubkey,
  distance, // maximum number of hops
);

// Returns: string[] | null - array of pubkeys (excluding the starting pubkey),
// or null if the starting pubkey doesn't exist in the graph
```

### Graph Statistics and Analysis

```typescript
// Get comprehensive graph statistics
const stats = await analyzer.getStats();
// Returns: {
//   totalFollows: number,
//   uniqueFollowers: number,
//   uniqueFollowed: number,
// }

// Get all unique pubkeys in the social graph
const allPubkeys = await analyzer.getAllUniquePubkeys();

// Check if a pubkey exists in the graph
const exists = await analyzer.pubkeyExists(pubkey);

// Check if a direct follow relationship exists
const isFollowing = await analyzer.isDirectFollow(
  followerPubkey,
  followedPubkey,
);

// Check if two pubkeys mutually follow each other
const areMutual = await analyzer.areMutualFollows(pubkey1, pubkey2);

// Get the degree (number of follows) for a pubkey
const degree = await analyzer.getPubkeyDegree(pubkey);
// Returns: { outDegree: number, inDegree: number }
```

## Data Model

The library uses a simple, efficient schema optimized for graph traversal:

```sql
CREATE TABLE nsd_follows (
    follower_pubkey VARCHAR(64) NOT NULL,
    followed_pubkey VARCHAR(64) NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (follower_pubkey, followed_pubkey)
);

-- Persistent root distances table for O(1) lookups
CREATE TABLE nsd_root_distances (
    pubkey VARCHAR(64) PRIMARY KEY,
    distance INTEGER NOT NULL
);

-- Metadata table for tracking root optimization state
CREATE TABLE nsd_metadata (
    key VARCHAR(64) PRIMARY KEY,
    value VARCHAR(255)
);
```

## Nostr Protocol Compliance

- ✅ **NIP-02** - Follow List specification
- ✅ **Event Replacement** - "Latest event wins" semantics
- ✅ **Pubkey Normalization** - Lowercase hex strings
- ✅ **Tag Validation** - Proper 'p' tag parsing

## Requirements

- Bun.js or Node.js 18+
- TypeScript 5+
- DuckDB Node API 1.4+

## Platform Compatibility

This library supports all major platforms through DuckDB's native bindings:

- **Linux x64** (Ubuntu, Debian, CentOS, etc.)
- **Linux arm64** (Raspberry Pi, AWS Graviton, etc.)
- **macOS x64** (Intel Macs)
- **macOS arm64** (Apple Silicon M1/M2/M3)
- **Windows x64** (Windows 10/11)

### ⚠️ Important Limitations

**This library is NOT intended for browser use** - it requires native DuckDB bindings that are only available in Node.js/Bun.js environments.

## Performance Tips

1. **Use Root Pubkey Optimization**: If you frequently query distances from a specific pubkey, set it as the root for O(1) lookups. The table is now persistent and supports delta updates.

2. **Use [`getShortestDistance()`](src/analyzer.ts:213) for Distance-Only Queries**: This is 2-3x faster than [`getShortestPath()`](src/analyzer.ts:189) when you only need the distance.

3. **Batch Distance Calculations**: Use [`getShortestDistancesBatch()`](src/analyzer.ts:255) for multiple distance queries from the same source.

4. **Leverage Delta Updates**: When using persistent databases, the root table is incrementally updated as new events are ingested, avoiding expensive full rebuilds.

5. **Reuse Persistent Tables**: When reconnecting to the same database, the root table is automatically reused if the root pubkey and maxDepth match, providing instant optimization.

## API Reference Updates

### New Methods for Root Table Management

```typescript
/**
 * Rebuilds the root distances table from scratch
 * Useful when you want to ensure the table is completely up-to-date
 */
rebuildRootDistances(): Promise<void>

/**
 * Drops the root distances table explicitly
 * This is now an explicit operation instead of automatic cleanup
 */
dropRootDistances(): Promise<void>
```

### Enhanced Root Pubkey Optimization

The [`setRootPubkey()`](src/analyzer.ts:575) method now:

- Reuses existing root tables when possible (same pubkey and maxDepth)
- Supports persistent tables that survive analyzer sessions
- Automatically applies delta updates when new events are ingested

## License

MIT

## Contributing

Contributions welcome! Please submit PRs.

## Acknowledgments

- Built with [DuckDB](https://duckdb.org/) for high-performance analytics
- Implements [Nostr](https://nostr.com/) protocol specifications
- Inspired by the need for efficient social graph analysis in decentralized networks

---

Made with 🦆 and 💛
