/** Tests if a string is hex */
export function isHex(str?: string) {
  if (str?.match(/^[0-9a-f]+$/i)) return true;
  return false;
}

/** Tests if a string is a 64 length hex string */
export function isHexKey(key?: string) {
  if (key?.toLowerCase()?.match(/^[0-9a-f]{64}$/)) return true;
  return false;
}
