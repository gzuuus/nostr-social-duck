# nostr-social-duck

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
