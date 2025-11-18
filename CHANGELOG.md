# nostr-social-duck

## 0.1.7

### Patch Changes

- perf(db): optimize ingestion with batching and transactions

  This commit significantly improves the performance of social graph ingestion by implementing batch processing, bulk operations, and transaction safety. Key improvements include:
  - Batch processing of events to handle large datasets efficiently
  - Bulk delete operations for faster follow relationship updates
  - Transaction wrapping for data integrity during ingestion
  - Optimized deduplication using Set instead of Map
  - Faster hex key validation utilities
  - Database reorganization (moved from examples/ to data/)

## 0.1.6

### Patch Changes

- feat: export all graph analysis methods from main library entry point

## 0.1.5

### Patch Changes

- feat: Add performance-optimized getShortestDistance method
  - Add new `getShortestDistance()` method that returns only distance, skipping expensive path reconstruction
  - Performance improvement: 2-3x faster for multi-hop paths by eliminating 2 additional recursive CTE queries
  - Maintain full backward compatibility with existing `getShortestPath()` method
  - Simplify bidirectional search depth logic for better consistency
  - Add comprehensive performance benchmarks using real Nostr social graph data
  - Update API documentation and types to include new method

## 0.1.4

### Patch Changes

- feat: add TypeScript declaration file generation
  - Update build process to generate .d.ts files alongside JavaScript bundle
  - Add TypeScript compiler as dev dependency
  - Configure tsconfig.json for proper declaration file generation
  - Streamline build script to handle both JS bundling and type declaration generation
  - Fix type resolution issues in external projects by providing proper type definitions

  The library now exports complete TypeScript types, resolving the "Could not find a declaration file for module 'nostr-social-duck'" error in consuming projects.

## 0.1.3

### Patch Changes

- fix: correct DuckDB native binding external dependencies in build script

  Update build script to include `/duckdb.node` suffix for all DuckDB native binding external dependencies, fixing ERR_DLOPEN_FAILED errors when using the library in other projects.

## 0.1.2

### Patch Changes

- feat: add getUsersWithinDistance method for social graph analysis
  - Implement `getUsersWithinDistance(fromPubkey, distance)` method that returns all pubkeys reachable within specified distance
  - Uses DuckDB's recursive CTE with USING KEY optimization for efficient traversal
  - Returns string[] of pubkeys (excluding starting pubkey)
  - Includes comprehensive unit tests covering edge cases, multi-hop traversal, and cycle prevention
  - Update API documentation and README with usage examples
  - Maintains consistency with existing codebase patterns and performance optimizations

## 0.1.1

### Patch Changes

- Initial release of nostr-social-duck library for Nostr social graph analysis using DuckDB.

  ### Features
  - High-performance graph traversal with DuckDB recursive CTEs
  - NIP-02 compliant follow list parsing
  - Efficient storage with strategic indexes
  - TypeScript support with comprehensive types

  ### Platform Notes
  - Currently supports Linux environments only
  - Not compatible with browsers
  - Requires Node.js 18+ or Bun.js
