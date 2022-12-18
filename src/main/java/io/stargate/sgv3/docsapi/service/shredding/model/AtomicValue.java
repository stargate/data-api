package io.stargate.sgv3.docsapi.service.shredding.model;

import java.math.BigDecimal;

/**
 * Simple immutable value container class used when shredding documents, to contain "full" value as
 * a {@link String} and providing access for getting hash for that value.
 *
 * <p>NOTE: not defined as key type or content comparable.
 */
public abstract class AtomicValue {
  private final DocValueType type;

  protected AtomicValue(DocValueType type) {
    this.type = type;
  }

  public abstract String typedValue();

  public abstract DocValueHash hashValue();

  // Mostly useful trouble-shooting, debug output
  @Override
  public String toString() {
    return "[Atomic:%c/%s]".formatted(type.prefix(), typedValue());
  }

  /**
   * Implementation used for simple static instances that represent {@link Boolean} values and
   * {@code null}s.
   */
  static final class Fixed extends AtomicValue {
    static final Fixed NULL = new Fixed(DocValueType.NULL, "");
    static final Fixed FALSE = new Fixed(DocValueType.BOOLEAN, "0");
    static final Fixed TRUE = new Fixed(DocValueType.BOOLEAN, "1");

    /** For simple fixed types the "full" and "hashed" values are one and same: */
    private final String typedValue;

    private final DocValueHash hashValue;

    private Fixed(DocValueType type, String value) {
      super(type);
      typedValue = type.prefix() + value;
      hashValue = new DocValueHash(typedValue);
    }

    @Override
    public String typedValue() {
      return typedValue;
    }

    @Override
    public DocValueHash hashValue() {
      return hashValue;
    }
  }

  /**
   * Implementation used for simple static instances that represent {@link String} and {@link
   * BigDecimal} values.
   */
  static final class Dynamic extends AtomicValue {
    private final String typedFullValue;

    private final DocValueHash hashValue;

    public static Dynamic forString(String str) {
      return new Dynamic(DocValueType.STRING, str);
    }

    public static Dynamic forNumber(BigDecimal num) {
      return new Dynamic(DocValueType.NUMBER, num.toPlainString());
    }

    Dynamic(DocValueType type, String value) {
      super(type);
      typedFullValue = type.prefix() + value;
      // !!! TODO: actual hashing
      hashValue = new DocValueHash(typedFullValue);
    }

    @Override
    public String typedValue() {
      return typedFullValue;
    }

    @Override
    public DocValueHash hashValue() {
      return hashValue;
    }
  }
}
