/** Tests if a string is hex */
export function isHex(str?: string): boolean {
  if (!str) return false;
  return /^[0-9a-f]+$/i.test(str);
}

/** Tests if a string is a 64 length hex string */
export function isHexKey(key?: string): boolean {
  // Fast path: check length first (cheapest operation)
  if (!key || key.length !== 64) return false;

  // Use test() instead of match() - it's faster as it doesn't create array
  return /^[0-9a-f]{64}$/i.test(key);
}

/**
 * Executes a database operation with retry logic for transaction conflicts
 * @param operation - Async function that performs the database operation
 * @param maxRetries - Maximum number of retry attempts (default: 3)
 * @returns Promise resolving to the operation result
 */
export async function executeWithRetry<T>(
  operation: () => Promise<T>,
  maxRetries: number = 3,
): Promise<T> {
  let retryCount = 0;

  while (true) {
    try {
      return await operation();
    } catch (error) {
      // Check if this is a transaction conflict that can be retried
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      const isRetryable =
        errorMessage.includes("Transaction conflict") ||
        errorMessage.includes("Failed to execute prepared statement");

      if (isRetryable && retryCount < maxRetries) {
        retryCount++;
        // Exponential backoff with minimal object creation
        await new Promise((resolve) =>
          setTimeout(resolve, Math.pow(2, retryCount) * 10),
        );
        continue;
      }
      throw error; // Re-throw if not retryable or max retries reached
    }
  }
}
