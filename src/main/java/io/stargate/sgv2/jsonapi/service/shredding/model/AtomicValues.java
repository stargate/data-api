package io.stargate.sgv2.jsonapi.service.shredding.model;

import java.math.BigDecimal;
import java.util.HashMap;

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

  public AtomicValue booleanValue(boolean b) {
    return b ? AtomicValue.TRUE : AtomicValue.FALSE;
  }

  public AtomicValue nullValue() {
    return AtomicValue.NULL;
  }
}
