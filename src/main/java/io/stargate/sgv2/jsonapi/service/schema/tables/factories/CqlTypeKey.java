package io.stargate.sgv2.jsonapi.service.schema.tables.factories;

import com.datastax.oss.driver.api.core.type.DataType;

/**
 * A key to identify a CQL type in the API,
 *
 * <p>The equals for collection data types in the driver does not take frozen into account. This is
 * a problem for the API because we do not support frozen types. So if we only used the DataType
 * from the driver the cache key for a frozen map of (string, string) would be the same for a
 * non-frozen version.
 *
 * <p>Create using the {@link #create(DataType, DataType, DataType, boolean)}
 */
public class CqlTypeKey {

  /** Type code to use when there is no type, e.g. for a non-collection type. */
  public static int NO_TYPE = 0xFF;

  private final int type;
  private final int valueType;
  private final int keyType;
  private final boolean isFrozen;
  private final int combinedCode;

  /**
   * Constructs a cache key for a data type combination.
   *
   * <p>All type values must be between 0 and 255 inclusive so they fit safely into one byte.
   * Otherwise, an exception is thrown.
   *
   * <p>The combinedCode layout is:
   *
   * <pre>
   * | 8 bits | 8 bits    | 8 bits   | 8 bits |
   * |--------|-----------|----------|--------|
   * | type   | valueType | keyType  | frozen |
   * </pre>
   *
   * The frozen bit is stored as 1 (true) or 0 (false).
   *
   * @param type The {@link com.datastax.oss.protocol.internal.ProtocolConstants.DataType} code for
   *     the type, must be > 0 and < 255 (not CUSTOM or NO_TYPE).
   * @param valueType The {@link com.datastax.oss.protocol.internal.ProtocolConstants.DataType} code
   *     for the value type for a collection that has one, must be > 0 and <=255. Use {@link
   *     #NO_TYPE} if not applicable.
   * @param keyType The {@link com.datastax.oss.protocol.internal.ProtocolConstants.DataType} code
   *     for the key type for a collection that has one, must be > 0 and <=255. Use {@link #NO_TYPE}
   *     if not applicable.
   * @param isFrozen <code>true</code> if the type is frozen, <code>false</code> otherwise.
   */
  public CqlTypeKey(int type, int valueType, int keyType, boolean isFrozen) {

    if (type < 1 || type >= 255) {
      throw new IllegalArgumentException("type must be between 1 and 254, but was: " + type);
    }
    this.type = type;
    this.valueType = validateValueKeyType(valueType, "valueType");
    this.keyType = validateValueKeyType(keyType, "keyType");
    this.isFrozen = isFrozen;
    this.combinedCode = buildCombinedCode(this.type, this.valueType, this.keyType, this.isFrozen);
  }

  /** Factory method to create a {@link CqlTypeKey} from the {@link DataType}s */
  public static CqlTypeKey create(
      DataType type, DataType valueType, DataType keyType, boolean isFrozen) {

    int typeCode = type.getProtocolCode();
    int valueTypeCode = valueType != null ? valueType.getProtocolCode() : NO_TYPE;
    int keyTypeCode = keyType != null ? keyType.getProtocolCode() : NO_TYPE;

    return new CqlTypeKey(typeCode, valueTypeCode, keyTypeCode, isFrozen);
  }

  private static int validateValueKeyType(int value, String fieldName) {
    if (value < 1 || value > 255) {
      throw new IllegalArgumentException(
          "Key or value type " + fieldName + " must be between 1 and 255, but was: " + value);
    }
    return value;
  }

  private static int buildCombinedCode(int type, int valueType, int keyType, boolean isFrozen) {
    int frozenBit = isFrozen ? 1 : 0;
    return (type << 24) | (valueType << 16) | (keyType << 8) | frozenBit;
  }

  @Override
  public int hashCode() {
    return combinedCode;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof CqlTypeKey)) {
      return false;
    }
    CqlTypeKey other = (CqlTypeKey) obj;
    return this.combinedCode == other.combinedCode;
  }

  @Override
  public String toString() {
    return "DataTypeCacheKey{"
        + "type="
        + type
        + ", valueType="
        + valueType
        + ", keyType="
        + keyType
        + ", isFrozen="
        + isFrozen
        + ", combinedCode=0x"
        + Integer.toHexString(combinedCode)
        + '}';
  }
}
