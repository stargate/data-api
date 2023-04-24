package io.stargate.sgv2.jsonapi.service.shredding.model;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;

/**
 * Simple per-shred-operation data structure for reusing already constructed {@link AtomicValue}
 * representations; this avoids hash recalculations and since values are retained anyway by caller
 * does not add significant memory retention (even for short term).
 *
 * <p>Note that maximum size not used because reuse/caching does not extend across shredding calls;
 * only values within a single document are reused.
 */
public class AtomicValues {
  final HashMap<Object, AtomicValue> seenValues = new HashMap<>();

  public AtomicValue stringValue(String value) {
    AtomicValue atomic = seenValues.get(value);
    if (atomic == null) {
      atomic = AtomicValue.forString(value);
      seenValues.put(value, atomic);
    }
    return atomic;
  }

  public AtomicValue numberValue(BigDecimal value) {
    AtomicValue atomic = seenValues.get(value);
    if (atomic == null) {
      atomic = AtomicValue.forNumber(value);
      seenValues.put(value, atomic);
    }
    return atomic;
  }

  public AtomicValue timestampValue(Date value) {
    AtomicValue atomic = seenValues.get(value);
    if (atomic == null) {
      atomic = AtomicValue.forTimestamp(value);
      seenValues.put(value, atomic);
    }
    return atomic;
  }

  public AtomicValue booleanValue(boolean b) {
    return b ? AtomicValue.TRUE : AtomicValue.FALSE;
  }

  public AtomicValue nullValue() {
    return AtomicValue.NULL;
  }
}
