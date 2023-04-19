package io.stargate.sgv2.jsonapi.service.shredding.model;

/**
 * Enumeration for supported Document value types: corresponds to accepted JSON types.
 *
 * <p>Prefix is used for non-digested "full" values to avoid value collisions (mostly between
 * Strings and Numbers): so number value {@code 25.0} would be encoded as {@code N25.0} whereas
 * String {@code "25.0"} as {@code S25.0}.
 *
 * <p>
 */
public enum DocValueType {
  ARRAY('A', false),
  OBJECT('O', false),

  BOOLEAN('B', true),
  NUMBER('N', true),
  NULL('Z', true),
  STRING('S', true),

  TIMESTAMP('T', true);

  private final char prefix;

  private final boolean atomic;

  private DocValueType(char prefix, boolean atomic) {
    this.prefix = prefix;
    this.atomic = atomic;
  }

  /**
   * @return Character used as prefix for full (stringified) values as well as non-hash-based
   *     digests. Has no semantics beyond having to be distinct (non-overlapping) across supported
   *     types.
   */
  public char prefix() {
    return prefix;
  }

  public boolean isAtomic() {
    return atomic;
  }
}
