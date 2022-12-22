package io.stargate.sgv3.docsapi.service.shredding.model;

/** Enumeration for supported Document value types: corresponds to accepted JSON types. */
public enum DocValueType {
  ARRAY('A', false),
  OBJECT('O', false),

  BOOLEAN('B', true),
  NUMBER('N', true),
  NULL('Z', true),
  STRING('S', true);

  private final char prefix;

  private final boolean atomic;

  private DocValueType(char prefix, boolean atomic) {
    this.prefix = prefix;
    this.atomic = atomic;
  }

  public char prefix() {
    return prefix;
  }

  public boolean isAtomic() {
    return atomic;
  }
}
