package io.stargate.sgv2.jsonapi.service.shredding.model;

import java.math.BigDecimal;

/**
 * Simple immutable value container class used when shredding documents, to contain "full" value as
 * a {@link String} and providing access for getting hash for that value. Full value is needed for
 * reconstructing documents ("de-shredding"), but only for atomic values, not structured (Array,
 * Object) as latter can be re-created from former.
 *
 * <p>NOTE: not defined as key type or content comparable.
 */
public record AtomicValue(DocValueType type, String typedFullValue, DocValueHash hash) {
  static final AtomicValue NULL = create(DocValueType.NULL, "");
  static final AtomicValue FALSE = create(DocValueType.BOOLEAN, "0");
  static final AtomicValue TRUE = create(DocValueType.BOOLEAN, "1");

  public static AtomicValue forString(String str) {
    // For Strings no changes needed, use default prefix+full-value
    return create(DocValueType.STRING, str);
  }

  public static AtomicValue forNumber(BigDecimal num) {
    // For Numbers just make sure not to use Engineering notation:
    return create(DocValueType.NUMBER, num.toPlainString());
  }

  static AtomicValue create(DocValueType type, String fullValue) {
    String typedFullValue = type.prefix() + fullValue;
    return new AtomicValue(
        type, typedFullValue, DocValueHash.constructBoundedHash(type, typedFullValue));
  }

  // Mostly useful trouble-shooting, debug output
  @Override
  public String toString() {
    return "[Atomic:%c/%s]".formatted(type.prefix(), typedFullValue());
  }
}
