# Nostr Social Duck 🦆

A high-performance TypeScript library for analyzing Nostr social graphs using DuckDB. Efficiently compute shortest paths and analyze follow relationships from Nostr Kind 3 events.

## Features

- 🚀 **Fast Graph Traversal** - Uses DuckDB's recursive CTEs with `USING KEY` optimization
- 📊 **Efficient Storage** - Optimized schema with strategic indexes
- 🔄 **NIP-02 Compliant** - Properly implements "latest event wins" semantics
- 💾 **Flexible Storage** - In-memory or persistent database options
- 🎯 **Type-Safe** - Full TypeScript support with comprehensive types

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

// Get graph statistics
const stats = await analyzer.getStats();
console.log(`Total follows: ${stats.totalFollows}`);

// Clean up
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
  maxDepth: 6, // Maximum search depth for paths
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

### Ingesting Events

```typescript
// Single event
await analyzer.ingestEvent(kind3Event);

// Multiple events (automatically deduplicates by pubkey)
await analyzer.ingestEvents([event1, event2, event3]);
```

### Finding Paths

```typescript
// Find shortest path
const path = await analyzer.getShortestPath(
  fromPubkey,
  toPubkey,
  maxDepth, // optional, defaults to analyzer's maxDepth
);

// Returns: { path: string[], distance: number, pubkeys: string[] } | null
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

### ⚠️ Important Limitations

**This library is NOT intended for browser use** and currently only works on **Linux environments**.

#### Future Platform Support

We plan to add support (if requested) for:

- macOS (Darwin) environments
- Windows environments
- WebAssembly builds for browser compatibility

For now, please use this library in Node.js/Bun.js environments on Linux systems.

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
