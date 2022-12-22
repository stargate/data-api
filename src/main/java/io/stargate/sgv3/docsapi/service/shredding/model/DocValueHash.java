package io.stargate.sgv3.docsapi.service.shredding.model;

/**
 * Value class that represents hash/digest of contents of a Document node (atomic value,
 * sub-document or array).
 *
 * <p>For atomic types calculated from String representation of the value (but not necessarily same
 * as what JSON uses) -- either type-prefixed value as-is (if short enough) or type-prefixed hash of
 * String representation (long String or Number values).
 *
 * <p>For structured types (Objects, Arrays), calculated from hashes/digest of directly contained
 * values: this will effectively calculated recursive hash/digest.
 */
public record DocValueHash(DocValueType type, boolean usesMD5, String hash) {
  @Override
  public String toString() {
    return hash;
  }

  /**
   * Helper method that will construct hash with bounded length (22 characters/bytes or less)
   *
   * @param valueType Type of the value being hashed
   * @param fullValue "Full" representation of the value (note: for Arrays and Objects includes
   *     hashes of child values, not full values)
   * @return DocValueHash constructed
   */
  public static DocValueHash constructBoundedHash(DocValueType valueType, String fullValue) {
    if (fullValue.length() < MD5Hasher.BASE64_ENCODED_MD5_LEN) {
      return new DocValueHash(valueType, false, fullValue);
    }
    // Actual MD5 hash is Base64-encoded (with no padding) to be stored in String
    // columns (or concatenated with other hashes for structured types).
    // We do NOT need to use type prefix because only hashed values have length of
    // 22 bytes/chars; non-hashed ("full") values used are always shorter.
    return new DocValueHash(valueType, true, MD5Hasher.hashAndBase64Encode(fullValue));
  }
}
