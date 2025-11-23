# Nostr Social Duck 🦆

A high-performance TypeScript library for analyzing Nostr social graphs using DuckDB. Efficiently compute shortest paths and analyze follow relationships from Nostr Kind 3 events.

## Features

- 🚀 **Fast Graph Traversal** - Uses DuckDB's recursive CTEs with `USING KEY` optimization
- 📊 **Efficient Storage** - Optimized schema with strategic indexes
- 🔄 **NIP-02 Compliant** - Properly implements "latest event wins" semantics
- 💾 **Flexible Storage** - In-memory or persistent database options
- 🎯 **Type-Safe** - Full TypeScript support with comprehensive types
- ⚡ **Root Pubkey Optimization** - Pre-computed distances for O(1) lookups

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

## API Reference

### Creating an Analyzer

```typescript
// In-memory database (default)
const analyzer = await DuckDBSocialGraphAnalyzer.create();

// Persistent database
const analyzer = await DuckDBSocialGraphAnalyzer.create({
  dbPath: "./social-graph.db",
  maxDepth: 6, // Maximum search depth for paths (Optional, default: 6)
});
```

### Using Existing DuckDB Connections

```typescript
// Connect to existing DuckDB instance
const connection = await myInstance.connect();
const analyzer = await DuckDBSocialGraphAnalyzer.connect(connection, 6);

// Use with custom maxDepth
const analyzer = await DuckDBSocialGraphAnalyzer.connect(connection, 10);
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
const path = await analyzer.getShortestPath(
  fromPubkey,
  toPubkey,
  maxDepth, // optional, defaults to analyzer's maxDepth
);

// Returns: { path: string[], distance: number } | null

// Find shortest distance only (2-3x faster)
const distance = await analyzer.getShortestDistance(
  fromPubkey,
  toPubkey,
  maxDepth, // optional, defaults to analyzer's maxDepth
);

// Returns: number | null - the distance in hops, or null if no path exists
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

### Graph Statistics

```typescript
const stats = await analyzer.getStats();
// Returns: {
//   totalFollows: number,
//   uniqueFollowers: number,
//   uniqueFollowed: number,
//   uniqueEvents: number
// }
```

### Getting All Unique Pubkeys

```typescript
// Get all unique pubkeys in the social graph (both followers and followed)
const allPubkeys = await analyzer.getAllUniquePubkeys();

// Returns: string[] - array of all unique pubkeys in the graph
```

### Additional Graph Analysis Methods

```typescript
// Check if a pubkey exists in the graph (as follower or followed)
const exists = await analyzer.pubkeyExists(pubkey);
// Returns: boolean

// Check if a direct follow relationship exists
const isFollowing = await analyzer.isDirectFollow(
  followerPubkey,
  followedPubkey,
);
// Returns: boolean

// Check if two pubkeys mutually follow each other
const areMutual = await analyzer.areMutualFollows(pubkey1, pubkey2);
// Returns: boolean

// Get the degree (number of follows) for a pubkey
const degree = await analyzer.getPubkeyDegree(pubkey);
// Returns: { outDegree: number, inDegree: number }

// Get all unique pubkeys in the social graph (both followers and followed)
const allPubkeys = await analyzer.getAllUniquePubkeys();
// Returns: string[] - array of all unique pubkeys in the graph

// Find the shortest distance between two pubkeys (performance-optimized)
const distance = await analyzer.getShortestDistance(
  fromPubkey,
  toPubkey,
  maxDepth,
);
// Returns: number | null - distance in hops, or null if no path exists
// Note: This is 2-3x faster than getShortestPath for multi-hop paths

// Root pubkey optimization for high-frequency queries
await analyzer.setRootPubkey("your_pubkey..."); // Pre-compute distances (stateful optimization)
const rootDistance = await analyzer.getShortestDistance(
  "your_pubkey...",
  "target_pubkey...",
); // O(1) lookup when querying from the root

// Get users at specific distance from root (requires root to be set)
const usersAtDistance = await analyzer.getUsersAtDistance(2); // All users exactly 2 hops away

// Get distance distribution from root (requires root to be set)
const distribution = await analyzer.getDistanceDistribution(); // {1: 150, 2: 2500, ...}
```

## Data Model

The library uses a simple, efficient schema:

```sql
CREATE TABLE nsd_follows (
    follower_pubkey VARCHAR(64) NOT NULL,
    followed_pubkey VARCHAR(64) NOT NULL,
    event_id VARCHAR(64) NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (follower_pubkey, followed_pubkey, event_id)
);
```

With strategic indexes for optimal graph traversal:

- `idx_nsd_follows_follower` - For outgoing edges
- `idx_nsd_follows_followed` - For incoming edges
- `idx_nsd_follows_compound` - For efficient lookups

## Architecture

### Core Components

- **Parser** ([`src/parser.ts`](src/parser.ts)) - NIP-02 compliant event parsing
- **Database** ([`src/database.ts`](src/database.ts)) - Schema and index management
- **Ingestion** ([`src/ingestion.ts`](src/ingestion.ts)) - Event processing with deduplication
- **Graph Analysis** ([`src/graph-analysis.ts`](src/graph-analysis.ts)) - Shortest path algorithms
- **Analyzer** ([`src/analyzer.ts`](src/analyzer.ts)) - Main public API

### Key Optimizations

1. **USING KEY in Recursive CTEs** - Prevents redundant path exploration
2. **Chunked Batch Inserts** - Handles large follow lists efficiently (500 items per chunk)
3. **Early Existence Checks** - Avoids expensive queries for non-existent nodes
4. **Strategic Indexes** - Optimized for both forward and backward graph traversal
5. **Cycle Prevention** - Uses `list_contains` to avoid infinite loops

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

## License

MIT

## Contributing

Contributions welcome! Please read our contributing guidelines and submit PRs.

## Acknowledgments

- Built with [DuckDB](https://duckdb.org/) for high-performance analytics
- Implements [Nostr](https://nostr.com/) protocol specifications
- Inspired by the need for efficient social graph analysis in decentralized networks

## Links

- [DuckDB Documentation](https://duckdb.org/docs/)
- [Nostr Protocol](https://github.com/nostr-protocol/nips)
- [NIP-02 Specification](https://github.com/nostr-protocol/nips/blob/master/02.md)

---

Made with 🦆 and ⚡ by the Nostr community
