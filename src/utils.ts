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
